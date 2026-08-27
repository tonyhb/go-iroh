package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tmc/go-iroh/internal/qng/internal/ackhandler"
	"github.com/tmc/go-iroh/internal/qng/internal/flowcontrol"
	"github.com/tmc/go-iroh/internal/qng/internal/monotime"
	"github.com/tmc/go-iroh/internal/qng/internal/protocol"
	"github.com/tmc/go-iroh/internal/qng/internal/wire"
)

// A SendStream is a unidirectional Send Stream.
type SendStream struct {
	mutex sync.Mutex

	numOutstandingFrames int64 // outstanding STREAM and RESET_STREAM frames
	retransmissionQueue  []*wire.StreamFrame

	ctx       context.Context
	ctxCancel context.CancelCauseFunc

	streamID protocol.StreamID
	sender   streamSender

	// reliableSize is the portion of the stream that needs to be transmitted reliably,
	// even if the stream is cancelled.
	// This requires the peer to support RESET_STREAM_AT.
	// This value should not be accessed directly, but only through the reliableOffset method.
	// This method returns 0 if the peer doesn't support the RESET_STREAM_AT extension.
	reliableSize protocol.ByteCount
	writeOffset  protocol.ByteCount

	shutdownErr            error
	resetErr               *StreamError
	queuedResetStreamFrame *wire.ResetStreamFrame

	supportsResetStreamAt bool
	finishedWriting       bool // set once Close() is called
	finSent               bool // set when a STREAM_FRAME with FIN bit has been sent
	// Set when the application knows about the cancellation.
	// This can happen because the application called CancelWrite,
	// or because Write returned the error (for remote cancellations).
	cancellationFlagged bool
	completed           bool // set when this stream has been reported to the streamSender as completed

	dataForWriting  []byte // during a Write() call, this slice is the part of p that still needs to be sent out
	writeBuffer     []byte
	writeBufferHead int
	// writeBufferLimit is the demand-grown buffer limit; zero means
	// sendStreamWriteBufferSize. It doubles toward
	// sendStreamWriteBufferMaxSize only on the full-buffer write path.
	writeBufferLimit int
	active           bool
	writesInEpisode  uint16
	burstUntil       monotime.Time
	corkPending      bool
	activationTimer  *time.Timer
	activationGen    uint64

	writeChan   chan struct{}
	writeActive bool
	writeWake   chan struct{}
	deadline    monotime.Time

	flowController flowcontrol.StreamFlowController
}

const (
	// sendStreamWriteBufferSize is the initial write buffer limit.
	sendStreamWriteBufferSize = 4096
	// sendStreamWriteBufferMaxSize caps the demand-grown write buffer
	// limit. Only streams under sustained full-buffer pressure reach it;
	// it bounds the worst-case buffered-unsent bytes on cancel.
	sendStreamWriteBufferMaxSize = 65536
	// maxBufferedWriteSize is the largest single write that buffers (and
	// grows the buffer). Larger writes keep the direct blocked-writer
	// path, whose frames copy straight from the caller's slice — routing
	// them through the buffer would add a full extra copy per write.
	maxBufferedWriteSize = sendStreamWriteBufferMaxSize / 4
)

var (
	_ io.ReaderFrom            = &SendStream{}
	_ streamControlFrameGetter = &SendStream{}
	_ outgoingStream           = &SendStream{}
	_ sendStreamFrameHandler   = &SendStream{}
)

func newSendStream(
	ctx context.Context,
	streamID protocol.StreamID,
	sender streamSender,
	flowController flowcontrol.StreamFlowController,
	supportsResetStreamAt bool,
) *SendStream {
	s := &SendStream{
		streamID:              streamID,
		sender:                sender,
		flowController:        flowController,
		writeChan:             make(chan struct{}, 1),
		writeWake:             make(chan struct{}, 1),
		supportsResetStreamAt: supportsResetStreamAt,
	}
	s.ctx, s.ctxCancel = context.WithCancelCause(ctx)
	return s
}

// StreamID returns the stream ID.
func (s *SendStream) StreamID() StreamID {
	return s.streamID // same for receiveStream and sendStream
}

// Write writes data to the stream.
// Write can be made to time out using [SendStream.SetWriteDeadline].
// If the stream was canceled, the error is a [StreamError].
func (s *SendStream) Write(p []byte) (int, error) {
	// Concurrent use of Write is not permitted (and doesn't make any sense),
	// but sometimes people do it anyway.
	// Make sure that we only execute one call at any given time to avoid hard to debug failures.
	s.mutex.Lock()
	// Steady-state fast path: no concurrent write episode, no error or
	// deadline condition, stream already active, and the write fits in the
	// buffer. The fast path never sets writeActive, so it creates no
	// waiters and must wake none; any condition failing falls through to
	// the general path unchanged.
	if !s.writeActive && s.resetErr == nil && s.shutdownErr == nil &&
		!s.finishedWriting && s.deadline.IsZero() && len(p) > 0 &&
		len(p) <= maxBufferedWriteSize &&
		s.active && s.growWriteBufferFor(len(p)) {
		s.appendWriteBuffer(p)
		if s.writesInEpisode < ^uint16(0) {
			s.writesInEpisode++
		}
		s.mutex.Unlock()
		return len(p), nil
	}
	for s.writeActive {
		s.mutex.Unlock()
		<-s.writeWake
		s.mutex.Lock()
	}
	s.writeActive = true

	isNewlyCompleted, n, err := s.writeLocked(p)
	if isNewlyCompleted {
		s.sender.onStreamCompleted(s.streamID)
	}
	return n, err
}

// ReadFrom implements [io.ReaderFrom]. It reads from r until EOF or error,
// writing to the stream in buffer-sized chunks so each write stays on the
// buffered fast path. Data is copied into stream-owned storage before each
// chunk write returns.
func (s *SendStream) ReadFrom(r io.Reader) (int64, error) {
	var total int64
	buf := make([]byte, sendStreamWriteBufferSize)
	for {
		n, rerr := r.Read(buf)
		if n > 0 {
			wn, werr := s.Write(buf[:n])
			total += int64(wn)
			if werr != nil {
				return total, werr
			}
		}
		if rerr == io.EOF {
			return total, nil
		}
		if rerr != nil {
			return total, rerr
		}
	}
}

// Writev writes the buffers in order, amortizing the per-call lock and
// bookkeeping across the vector. It returns the total number of bytes
// written and advances bufs to reflect exactly what was consumed,
// including a partially written element, so the caller can resume after a
// short write. The stream copies data into owned storage before Writev
// returns; the caller may reuse the underlying slices immediately.
//
// Writev does not hold the stream for the whole vector. Elements too large
// for the write buffer are delegated to Write, and another writer may
// interleave between elements. The delivered byte stream is identical to
// the equivalent sequence of Write calls.
//
// To send a [net.Buffers], call Writev directly: [net.Buffers] implements
// [io.WriterTo], which [io.Copy] prefers over [io.ReaderFrom], so
// io.Copy(stream, &bufs) degrades to one Write call per element and never
// batches.
//
// For burst accounting, a Writev counts as one write for the group of
// elements appended to the write buffer plus one per delegated element, so
// a vector of mixed sizes is not accounted the same as either one Write or
// N Writes.
//
// Writev is intended to be no slower than the equivalent sequence of Write
// calls; whether it is faster depends on the batch depth and on how many
// elements exceed the write buffer.
func (s *SendStream) Writev(bufs *net.Buffers) (int64, error) {
	total, err := s.writeVectored(*bufs)
	consumed := total
	for consumed > 0 && len(*bufs) > 0 {
		if n := int64(len((*bufs)[0])); consumed >= n {
			consumed -= n
			*bufs = (*bufs)[1:]
			continue
		}
		(*bufs)[0] = (*bufs)[0][consumed:]
		consumed = 0
	}
	if total > 0 && len(*bufs) > 0 && err == nil {
		// Unreachable today (writeVectored only stops early on error),
		// kept so a future short write cannot silently desynchronize bufs.
		err = io.ErrShortWrite
	}
	return total, err
}

// writeVectored writes the elements of bufs in order. Elements that fit the
// write buffer in the steady state are appended under a single mutex hold;
// any element that does not is delegated to Write, which handles activation,
// blocking, deadlines, and errors. The delivered byte stream is identical to
// the equivalent sequence of Write calls.
func (s *SendStream) writeVectored(bufs [][]byte) (int64, error) {
	var total int64
	s.mutex.Lock()
	appended := false
	for i := 0; i < len(bufs); {
		p := bufs[i]
		if len(p) == 0 {
			i++
			continue
		}
		if !s.writeActive && s.resetErr == nil && s.shutdownErr == nil &&
			!s.finishedWriting && s.deadline.IsZero() &&
			s.active && len(p) <= maxBufferedWriteSize && s.growWriteBufferFor(len(p)) {
			s.appendWriteBuffer(p)
			appended = true
			total += int64(len(p))
			i++
			continue
		}
		s.mutex.Unlock()
		n, err := s.Write(p)
		total += int64(n)
		if err != nil {
			return total, err
		}
		i++
		s.mutex.Lock()
	}
	if appended && s.writesInEpisode < ^uint16(0) {
		s.writesInEpisode++
	}
	s.mutex.Unlock()
	return total, nil
}

// writeLocked writes p. The caller holds s.mutex and owns the write operation.
func (s *SendStream) writeLocked(p []byte) (bool /* is newly completed */, int, error) {
	if s.resetErr != nil {
		s.cancellationFlagged = true
		completed := s.isNewlyCompleted()
		err := s.resetErr
		s.finishWriteLocked()
		return completed, 0, err
	}
	if s.shutdownErr != nil {
		err := s.shutdownErr
		s.finishWriteLocked()
		return false, 0, err
	}
	if s.finishedWriting {
		s.finishWriteLocked()
		return false, 0, fmt.Errorf("write on closed stream %d", s.streamID)
	}
	if !s.deadline.IsZero() && !monotime.Now().Before(s.deadline) {
		s.finishWriteLocked()
		return false, 0, errDeadline
	}
	if len(p) == 0 {
		s.finishWriteLocked()
		return false, 0, nil
	}

	s.dataForWriting = p

	var (
		deadlineTimer  *time.Timer
		bytesWritten   int
		notifiedSender bool
	)
	for {
		var copied bool
		var deadline monotime.Time
		if s.shutdownErr != nil || s.resetErr != nil {
			break
		}
		// Copy a bounded tail so Write can return before the bytes are packetized.
		// Larger writes retain the direct blocked-writer path.
		canBuffer := s.canBufferWrite()
		if !canBuffer && len(p) <= maxBufferedWriteSize {
			canBuffer = s.growWriteBufferFor(len(s.dataForWriting))
		}
		if canBuffer && len(s.dataForWriting) > 0 {
			s.appendWriteBuffer(s.dataForWriting)
			s.dataForWriting = nil
			bytesWritten = len(p)
			copied = true
			if s.writesInEpisode < ^uint16(0) {
				s.writesInEpisode++
			}
		} else {
			bytesWritten = len(p) - len(s.dataForWriting)
			deadline = s.deadline
			if !deadline.IsZero() {
				if !monotime.Now().Before(deadline) {
					s.dataForWriting = nil
					s.finishWriteLocked()
					return false, bytesWritten, errDeadline
				}
				if deadlineTimer == nil {
					deadlineTimer = time.NewTimer(monotime.Until(deadline))
					defer deadlineTimer.Stop()
				} else {
					deadlineTimer.Reset(monotime.Until(deadline))
				}
			}
			if s.dataForWriting == nil || s.shutdownErr != nil || s.resetErr != nil {
				break
			}
		}

		notifySender := false
		if !notifiedSender {
			notifiedSender = true
			if !s.active {
				notifySender = s.activateOrDelayLocked()
			}
		}
		if copied {
			s.writeActive = false
		}
		s.mutex.Unlock()
		if notifySender {
			s.sender.onHasStreamData(s.streamID, s)
		}
		if copied {
			s.wakeWriter()
			return false, bytesWritten, nil
		}
		if deadline.IsZero() {
			<-s.writeChan
		} else {
			select {
			case <-s.writeChan:
			case <-deadlineTimer.C:
			}
		}
		s.mutex.Lock()
	}

	if bytesWritten == len(p) {
		s.finishWriteLocked()
		return false, bytesWritten, nil
	}
	if s.shutdownErr != nil {
		err := s.shutdownErr
		s.finishWriteLocked()
		return false, bytesWritten, err
	}
	if s.resetErr != nil {
		s.cancellationFlagged = true
		completed := s.isNewlyCompleted()
		err := s.resetErr
		s.finishWriteLocked()
		return completed, bytesWritten, err
	}
	s.finishWriteLocked()
	return false, bytesWritten, nil
}

func (s *SendStream) finishWriteLocked() {
	s.writeActive = false
	s.mutex.Unlock()
	s.wakeWriter()
}

func (s *SendStream) wakeWriter() {
	select {
	case s.writeWake <- struct{}{}:
	default:
	}
}

func (s *SendStream) bufferedWriteLen() int {
	return len(s.writeBuffer) - s.writeBufferHead
}

// writeBufferLimitLocked returns the current demand-grown buffer limit
// without growing it. The caller holds s.mutex.
func (s *SendStream) writeBufferLimitLocked() int {
	if s.writeBufferLimit == 0 {
		return sendStreamWriteBufferSize
	}
	return s.writeBufferLimit
}

// growWriteBufferFor reports whether n more bytes fit the write buffer,
// doubling the demand-grown limit toward the cap when they do not.
// Growth happens only here — on the full-buffer write path — never on
// stream open or on the drain side. The caller holds s.mutex.
func (s *SendStream) growWriteBufferFor(n int) bool {
	limit := s.writeBufferLimitLocked()
	need := s.bufferedWriteLen() + n
	for need > limit && limit < sendStreamWriteBufferMaxSize {
		limit *= 2
	}
	s.writeBufferLimit = limit
	return need <= limit
}

// canBufferWrite reports room under the current limit without growing;
// the drain side uses it to decide when to wake a blocked writer.
func (s *SendStream) canBufferWrite() bool {
	return s.bufferedWriteLen()+len(s.dataForWriting) <= s.writeBufferLimitLocked()
}

func (s *SendStream) appendWriteBuffer(p []byte) {
	if s.writeBuffer == nil {
		s.writeBuffer = make([]byte, 0, sendStreamWriteBufferSize)
	}
	if cap(s.writeBuffer)-len(s.writeBuffer) < len(p) {
		copy(s.writeBuffer, s.writeBuffer[s.writeBufferHead:])
		s.writeBuffer = s.writeBuffer[:s.bufferedWriteLen()]
		s.writeBufferHead = 0
	}
	s.writeBuffer = append(s.writeBuffer, p...)
}

func (s *SendStream) activateOrDelayLocked() bool {
	if !s.corkPending {
		if s.burstUntil.IsZero() || !monotime.Now().Before(s.burstUntil) {
			s.burstUntil = 0
			s.active = true
			return true
		}
		s.corkPending = true
	}
	if sendStreamTailDelay <= 0 || s.bufferedWriteLen() >= sendStreamActivationThreshold {
		s.stopActivationTimerLocked()
		s.burstUntil = 0
		s.corkPending = false
		s.active = true
		return true
	}
	if s.activationTimer == nil {
		s.activationGen++
		gen := s.activationGen
		s.activationTimer = time.AfterFunc(sendStreamTailDelay, func() {
			s.activateAfterDelay(gen)
		})
	}
	return false
}

func (s *SendStream) stopActivationTimerLocked() {
	if s.activationTimer == nil {
		return
	}
	s.activationTimer.Stop()
	s.activationTimer = nil
	s.activationGen++
}

func (s *SendStream) activateAfterDelay(gen uint64) {
	s.mutex.Lock()
	if gen != s.activationGen {
		s.mutex.Unlock()
		return
	}
	s.activationTimer = nil
	if s.active || s.shutdownErr != nil || s.resetErr != nil ||
		(s.bufferedWriteLen() == 0 && s.dataForWriting == nil) {
		s.mutex.Unlock()
		return
	}
	s.burstUntil = 0
	s.corkPending = false
	s.active = true
	s.mutex.Unlock()
	recordCorkTimerActivation()
	s.sender.onHasStreamData(s.streamID, s)
}

// popStreamFrame returns the next STREAM frame that is supposed to be sent on this stream
// maxBytes is the maximum length this frame (including frame header) will have.
func (s *SendStream) popStreamFrame(maxBytes protocol.ByteCount, v protocol.Version) (_ ackhandler.StreamFrame, _ *wire.StreamDataBlockedFrame, hasMore bool) {
	s.mutex.Lock()
	f, blocked, hasMoreData := s.popNewOrRetransmittedStreamFrame(maxBytes, v)
	if f != nil {
		s.numOutstandingFrames++
	}
	if !hasMoreData {
		if s.writesInEpisode >= sendStreamBurstMinWrites {
			s.burstUntil = monotime.Now().Add(sendStreamBurstFreshness)
		} else {
			s.burstUntil = 0
		}
		s.writesInEpisode = 0
		s.corkPending = false
		s.active = false
	}
	s.mutex.Unlock()

	if f == nil {
		return ackhandler.StreamFrame{}, blocked, hasMoreData
	}
	return ackhandler.StreamFrame{
		Frame:   f,
		Handler: (*sendStreamAckHandler)(s),
	}, blocked, hasMoreData
}

func (s *SendStream) notifyHasStreamData() {
	s.mutex.Lock()
	s.stopActivationTimerLocked()
	s.burstUntil = 0
	s.corkPending = false
	if s.active {
		s.mutex.Unlock()
		return
	}
	s.active = true
	s.mutex.Unlock()
	s.sender.onHasStreamData(s.streamID, s)
}

func (s *SendStream) popNewOrRetransmittedStreamFrame(maxBytes protocol.ByteCount, v protocol.Version) (_ *wire.StreamFrame, _ *wire.StreamDataBlockedFrame, hasMoreData bool) {
	if s.shutdownErr != nil {
		return nil, nil, false
	}
	if s.resetErr != nil {
		reliableOffset := s.reliableOffset()
		if reliableOffset == 0 || (s.writeOffset >= reliableOffset && len(s.retransmissionQueue) == 0) {
			return nil, nil, false
		}
	}

	if len(s.retransmissionQueue) > 0 {
		f, hasMoreRetransmissions := s.maybeGetRetransmission(maxBytes, v)
		if f != nil || hasMoreRetransmissions {
			if f == nil {
				return nil, nil, true
			}
			// We always claim that we have more data to send.
			// This might be incorrect, in which case there'll be a spurious call to popStreamFrame in the future.
			return f, nil, true
		}
	}

	if len(s.dataForWriting) == 0 && s.bufferedWriteLen() == 0 {
		if s.finishedWriting && !s.finSent {
			s.finSent = true
			return &wire.StreamFrame{
				StreamID:       s.streamID,
				Offset:         s.writeOffset,
				DataLenPresent: true,
				Fin:            true,
			}, nil, false
		}
		return nil, nil, false
	}

	maxDataLen := s.flowController.SendWindowSize()
	if maxDataLen == 0 {
		return nil, nil, true
	}

	// if the stream is canceled, only data up to the reliable size needs to be sent
	reliableOffset := s.reliableOffset()
	if s.resetErr != nil && reliableOffset > 0 {
		maxDataLen = min(maxDataLen, reliableOffset-s.writeOffset)
	}
	f, hasMoreData := s.popNewStreamFrame(maxBytes, maxDataLen, v)
	if f == nil {
		return nil, nil, hasMoreData
	}
	if f.DataLen() > 0 {
		s.writeOffset += f.DataLen()
		s.flowController.AddBytesSent(f.DataLen())
	}
	if s.resetErr != nil && s.writeOffset >= reliableOffset {
		hasMoreData = false
	}
	var blocked *wire.StreamDataBlockedFrame
	// If the entire send window is used, the stream might have become blocked on stream-level flow control.
	// This is not guaranteed though, because the stream might also have been blocked on connection-level flow control.
	if f.DataLen() == maxDataLen && s.flowController.IsNewlyBlocked() {
		blocked = &wire.StreamDataBlockedFrame{StreamID: s.streamID, MaximumStreamData: s.writeOffset}
	}
	f.Fin = s.finishedWriting && s.dataForWriting == nil && s.bufferedWriteLen() == 0 && !s.finSent
	if f.Fin {
		s.finSent = true
	}
	return f, blocked, hasMoreData
}

// popNewStreamFrame returns a new STREAM frame to send for this stream
// hasMoreData says if there's more data to send, *not* taking into account the reliable size
func (s *SendStream) popNewStreamFrame(maxBytes, maxDataLen protocol.ByteCount, v protocol.Version) (_ *wire.StreamFrame, hasMoreData bool) {
	f := wire.GetStreamFrame()
	f.Fin = false
	f.StreamID = s.streamID
	f.Offset = s.writeOffset
	f.DataLenPresent = true
	f.Data = f.Data[:0]

	maxDataLen = min(maxDataLen, f.MaxDataLen(maxBytes, v))
	if maxDataLen == 0 {
		f.PutBack()
		return nil, true
	}
	if n := min(s.bufferedWriteLen(), int(maxDataLen)); n > 0 {
		f.Data = f.Data[:n]
		copy(f.Data, s.writeBuffer[s.writeBufferHead:s.writeBufferHead+n])
		s.writeBufferHead += n
		if s.writeBufferHead == len(s.writeBuffer) {
			s.writeBuffer = s.writeBuffer[:0]
			s.writeBufferHead = 0
		}
		s.signalWrite()
		hasMoreData = s.bufferedWriteLen() > 0 || s.dataForWriting != nil
	} else {
		s.getDataForWriting(f, maxDataLen)
		hasMoreData = s.dataForWriting != nil || s.finishedWriting
	}
	if len(f.Data) == 0 && !f.Fin {
		f.PutBack()
		return nil, hasMoreData
	}
	return f, hasMoreData
}

func (s *SendStream) maybeGetRetransmission(maxBytes protocol.ByteCount, v protocol.Version) (*wire.StreamFrame, bool /* has more retransmissions */) {
	f := s.retransmissionQueue[0]
	newFrame, needsSplit := f.MaybeSplitOffFrame(maxBytes, v)
	if needsSplit {
		return newFrame, true
	}
	s.retransmissionQueue = s.retransmissionQueue[1:]
	return f, len(s.retransmissionQueue) > 0
}

func (s *SendStream) getDataForWriting(f *wire.StreamFrame, maxBytes protocol.ByteCount) {
	if protocol.ByteCount(len(s.dataForWriting)) <= maxBytes {
		f.Data = f.Data[:len(s.dataForWriting)]
		copy(f.Data, s.dataForWriting)
		s.dataForWriting = nil
		s.signalWrite()
		return
	}
	f.Data = f.Data[:maxBytes]
	copy(f.Data, s.dataForWriting)
	s.dataForWriting = s.dataForWriting[maxBytes:]
	if s.canBufferWrite() {
		s.signalWrite()
	}
}

func (s *SendStream) isNewlyCompleted() bool {
	if s.completed {
		return false
	}
	if s.bufferedWriteLen() > 0 {
		return false
	}
	// We need to keep the stream around until all frames have been sent and acknowledged.
	if s.numOutstandingFrames > 0 || len(s.retransmissionQueue) > 0 || s.queuedResetStreamFrame != nil {
		return false
	}
	// The stream is completed if we sent the FIN.
	if s.finSent {
		s.completed = true
		return true
	}
	// The stream is also completed if:
	// 1. the application called CancelWrite, or
	// 2. we received a STOP_SENDING, and
	// 		* the application consumed the error via Write, or
	//		* the application called Close
	if s.resetErr != nil && (s.cancellationFlagged || s.finishedWriting) {
		s.completed = true
		return true
	}
	return false
}

// Close closes the write-direction of the stream.
// Future calls to Write are not permitted after calling Close.
// It must not be called concurrently with Write.
// It must not be called after calling CancelWrite.
func (s *SendStream) Close() error {
	s.mutex.Lock()
	if s.shutdownErr != nil || s.finishedWriting {
		s.mutex.Unlock()
		return nil
	}
	s.finishedWriting = true
	cancelled := s.resetErr != nil
	if cancelled {
		s.cancellationFlagged = true
	}
	completed := s.isNewlyCompleted()
	s.mutex.Unlock()

	if completed {
		s.sender.onStreamCompleted(s.streamID)
	}
	if cancelled {
		return fmt.Errorf("close called for canceled stream %d", s.streamID)
	}
	s.notifyHasStreamData()

	s.ctxCancel(nil)
	return nil
}

// SetReliableBoundary marks the data written to this stream so far as reliable.
// It is valid to call this function multiple times, thereby increasing the reliable size.
// It only has an effect if the peer enabled support for the RESET_STREAM_AT extension,
// otherwise, it is a no-op.
func (s *SendStream) SetReliableBoundary() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.reliableSize = s.writeOffset
	s.reliableSize += protocol.ByteCount(s.bufferedWriteLen())
}

// returnFramesToPool returns all queued frames to the sync.Pool
func (s *SendStream) returnFramesToPool() {
	s.stopActivationTimerLocked()
	s.burstUntil = 0
	s.corkPending = false
	for _, f := range s.retransmissionQueue {
		f.PutBack()
	}
	clear(s.retransmissionQueue)
	s.retransmissionQueue = nil
	s.writeBuffer = nil
	s.writeBufferHead = 0
}

// CancelWrite aborts sending on this stream.
// Data already written, but not yet delivered to the peer is not guaranteed to be delivered reliably.
// Write will unblock immediately, and future calls to Write will fail.
// When called multiple times it is a no-op.
// When called after Close, it aborts reliable delivery of outstanding stream data.
// Note that there is no guarantee if the peer will receive the FIN or the cancellation error first.
func (s *SendStream) CancelWrite(errorCode StreamErrorCode) {
	s.mutex.Lock()
	if s.shutdownErr != nil {
		s.mutex.Unlock()
		return
	}

	s.cancellationFlagged = true

	if s.resetErr != nil {
		completed := s.isNewlyCompleted()
		s.mutex.Unlock()
		// The user has called CancelWrite. If the previous cancellation was because of a
		// STOP_SENDING, we don't need to flag the error to the user anymore.
		if completed {
			s.sender.onStreamCompleted(s.streamID)
		}
		return
	}
	s.resetErr = &StreamError{StreamID: s.streamID, ErrorCode: errorCode, Remote: false}
	s.ctxCancel(s.resetErr)

	reliableOffset := s.reliableOffset()
	if reliableOffset == 0 {
		s.numOutstandingFrames = 0
		s.returnFramesToPool()
	}
	s.queuedResetStreamFrame = &wire.ResetStreamFrame{
		StreamID:  s.streamID,
		FinalSize: max(s.writeOffset, reliableOffset),
		ErrorCode: errorCode,
		// if the peer doesn't support the extension, the reliable offset will always be 0
		ReliableSize: reliableOffset,
	}
	if reliableOffset > 0 {
		if buffered := protocol.ByteCount(s.bufferedWriteLen()); buffered > 0 {
			keep := reliableOffset - s.writeOffset
			if keep <= 0 {
				s.writeBuffer = s.writeBuffer[:0]
				s.writeBufferHead = 0
			} else if keep < buffered {
				s.writeBuffer = s.writeBuffer[:s.writeBufferHead+int(keep)]
			}
		}
		if len(s.retransmissionQueue) > 0 {
			retransmissionQueue := make([]*wire.StreamFrame, 0, len(s.retransmissionQueue))
			for _, f := range s.retransmissionQueue {
				if f.Offset >= reliableOffset {
					f.PutBack()
					continue
				}
				if f.Offset+f.DataLen() <= reliableOffset {
					retransmissionQueue = append(retransmissionQueue, f)
				} else {
					f.Data = f.Data[:reliableOffset-f.Offset]
					retransmissionQueue = append(retransmissionQueue, f)
				}
			}
			s.retransmissionQueue = retransmissionQueue
		}
	}
	s.mutex.Unlock()

	s.signalWrite()
	s.sender.onHasStreamControlFrame(s.streamID, s)
}

func (s *SendStream) enableResetStreamAt() {
	s.mutex.Lock()
	s.supportsResetStreamAt = true
	s.mutex.Unlock()
}

func (s *SendStream) updateSendWindow(limit protocol.ByteCount) {
	updated := s.flowController.UpdateSendWindow(limit)
	if !updated { // duplicate or reordered MAX_STREAM_DATA frame
		return
	}
	s.mutex.Lock()
	hasStreamData := s.dataForWriting != nil || s.bufferedWriteLen() > 0
	s.mutex.Unlock()
	if hasStreamData {
		s.notifyHasStreamData()
	}
}

func (s *SendStream) onConnectionSendWindowUpdated() {
	s.mutex.Lock()
	hasStreamData := s.dataForWriting != nil || s.bufferedWriteLen() > 0
	s.mutex.Unlock()
	if hasStreamData {
		s.notifyHasStreamData()
	}
}

func (s *SendStream) handleStopSendingFrame(f *wire.StopSendingFrame) {
	s.mutex.Lock()
	if s.shutdownErr != nil {
		s.mutex.Unlock()
		return
	}

	// If the stream was already cancelled (either locally, or due to a previous STOP_SENDING frame),
	// there's nothing else to do.
	if s.resetErr != nil && s.reliableOffset() == 0 {
		s.mutex.Unlock()
		return
	}
	// if the peer stopped reading from the stream, there's no need to transmit any data reliably
	s.reliableSize = 0
	s.numOutstandingFrames = 0
	s.returnFramesToPool()
	if s.resetErr == nil {
		s.resetErr = &StreamError{StreamID: s.streamID, ErrorCode: f.ErrorCode, Remote: true}
		s.ctxCancel(s.resetErr)
	}
	s.queuedResetStreamFrame = &wire.ResetStreamFrame{
		StreamID:  s.streamID,
		FinalSize: s.writeOffset,
		ErrorCode: s.resetErr.ErrorCode,
	}
	s.mutex.Unlock()

	s.signalWrite()
	s.sender.onHasStreamControlFrame(s.streamID, s)
}

func (s *SendStream) getControlFrame(monotime.Time) (_ ackhandler.Frame, ok, hasMore bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.queuedResetStreamFrame == nil {
		return ackhandler.Frame{}, false, false
	}
	s.numOutstandingFrames++
	f := ackhandler.Frame{
		Frame:   s.queuedResetStreamFrame,
		Handler: (*sendStreamResetStreamHandler)(s),
	}
	s.queuedResetStreamFrame = nil
	return f, true, false
}

func (s *SendStream) reliableOffset() protocol.ByteCount {
	if !s.supportsResetStreamAt {
		return 0
	}
	return s.reliableSize
}

// The Context is canceled as soon as the write-side of the stream is closed.
// This happens when Close() or CancelWrite() is called, or when the peer
// cancels the read-side of their stream.
// The cancellation cause is set to the error that caused the stream to
// close, or `context.Canceled` in case the stream is closed without error.
func (s *SendStream) Context() context.Context {
	return s.ctx
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
// Even if write times out, it may return n > 0, indicating that
// some data was successfully written.
// A zero value for t means Write will not time out.
func (s *SendStream) SetWriteDeadline(t time.Time) error {
	s.mutex.Lock()
	s.deadline = monotime.FromTime(t)
	s.mutex.Unlock()
	s.signalWrite()
	return nil
}

// CloseForShutdown closes a stream abruptly.
// It makes Write unblock (and return the error) immediately.
// The peer will NOT be informed about this: the stream is closed without sending a FIN or RST.
func (s *SendStream) closeForShutdown(err error) {
	s.mutex.Lock()
	if s.shutdownErr == nil && !s.finishedWriting {
		s.shutdownErr = err
		s.returnFramesToPool()
	}
	s.mutex.Unlock()
	s.signalWrite()
}

// signalWrite performs a non-blocking send on the writeChan
func (s *SendStream) signalWrite() {
	select {
	case s.writeChan <- struct{}{}:
	default:
	}
}

type sendStreamAckHandler SendStream

var _ ackhandler.FrameHandler = &sendStreamAckHandler{}

func (s *sendStreamAckHandler) OnAcked(f wire.Frame) {
	sf := f.(*wire.StreamFrame)
	sf.PutBack()

	s.mutex.Lock()
	if s.resetErr != nil && (*SendStream)(s).reliableOffset() == 0 {
		s.mutex.Unlock()
		return
	}
	s.numOutstandingFrames--
	if s.numOutstandingFrames < 0 {
		panic("numOutStandingFrames negative")
	}
	completed := (*SendStream)(s).isNewlyCompleted()
	s.mutex.Unlock()

	if completed {
		s.sender.onStreamCompleted(s.streamID)
	}
}

func (s *sendStreamAckHandler) OnLost(f wire.Frame) {
	sf := f.(*wire.StreamFrame)
	s.mutex.Lock()
	// If the reliable size was 0 when the stream was cancelled,
	// the number of outstanding frames was immediately set to 0, and the retransmission queue was dropped.
	if s.resetErr != nil && (*SendStream)(s).reliableOffset() == 0 {
		// Return the frame to pool since it won't be retransmitted
		sf.PutBack()
		s.mutex.Unlock()
		return
	}
	s.numOutstandingFrames--
	if s.numOutstandingFrames < 0 {
		panic("numOutStandingFrames negative")
	}

	if s.resetErr != nil && (*SendStream)(s).reliableOffset() > 0 {
		// If the stream was reset, and this frame is beyond the reliable offset,
		// it doesn't need to be retransmitted.
		if sf.Offset >= (*SendStream)(s).reliableOffset() {
			sf.PutBack()
			// If this frame was the last one tracked, losing it might cause the stream to be completed.
			completed := (*SendStream)(s).isNewlyCompleted()
			s.mutex.Unlock()
			if completed {
				s.sender.onStreamCompleted(s.streamID)
			}
			return
		}
		// If the payload of the frame extends beyond the reliable size,
		// truncate the frame to the reliable size.
		if sf.Offset+sf.DataLen() > (*SendStream)(s).reliableOffset() {
			sf.Data = sf.Data[:(*SendStream)(s).reliableOffset()-sf.Offset]
		}
	}

	sf.DataLenPresent = true
	s.retransmissionQueue = append(s.retransmissionQueue, sf)
	s.mutex.Unlock()

	(*SendStream)(s).notifyHasStreamData()
}

type sendStreamResetStreamHandler SendStream

var _ ackhandler.FrameHandler = &sendStreamResetStreamHandler{}

func (s *sendStreamResetStreamHandler) OnAcked(f wire.Frame) {
	rsf := f.(*wire.ResetStreamFrame)
	s.mutex.Lock()
	// If the peer sent a STOP_SENDING after we sent a RESET_STREAM_AT frame,
	// we sent 1. reduced the reliable size to 0 and 2. sent a RESET_STREAM frame.
	// In this case, we don't care about the acknowledgment of this frame.
	if rsf.ReliableSize != (*SendStream)(s).reliableOffset() {
		s.mutex.Unlock()
		return
	}
	s.numOutstandingFrames--
	if s.numOutstandingFrames < 0 {
		panic("numOutStandingFrames negative")
	}
	completed := (*SendStream)(s).isNewlyCompleted()
	s.mutex.Unlock()

	if completed {
		s.sender.onStreamCompleted(s.streamID)
	}
}

func (s *sendStreamResetStreamHandler) OnLost(f wire.Frame) {
	rsf := f.(*wire.ResetStreamFrame)
	s.mutex.Lock()
	// If the peer sent a STOP_SENDING after we sent a RESET_STREAM_AT frame,
	// we sent 1. reduced the reliable size to 0 and 2. sent a RESET_STREAM frame.
	// In this case, the loss of the RESET_STREAM_AT frame can be ignored.
	if rsf.ReliableSize != (*SendStream)(s).reliableOffset() {
		s.mutex.Unlock()
		return
	}
	s.queuedResetStreamFrame = rsf
	s.numOutstandingFrames--
	s.mutex.Unlock()
	s.sender.onHasStreamControlFrame(s.streamID, (*SendStream)(s))
}
