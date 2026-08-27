package socket

import "sync/atomic"

// Metrics is the magic socket's datagram counter set.
type Metrics struct {
	updateDirectAddrs           atomic.Uint64
	ipv4Sent                    atomic.Uint64
	ipv6Sent                    atomic.Uint64
	relaySent                   atomic.Uint64
	customSent                  atomic.Uint64
	endpointIDSent              atomic.Uint64
	blackholed                  atomic.Uint64
	ipv4Recv                    atomic.Uint64
	ipv6Recv                    atomic.Uint64
	relayRecv                   atomic.Uint64
	customRecv                  atomic.Uint64
	recvDatagrams               atomic.Uint64
	relayHomeChange             atomic.Uint64
	relayRateLimited            atomic.Uint64
	holepunchAttempts           atomic.Uint64
	pathsDirect                 atomic.Uint64
	pathsRelay                  atomic.Uint64
	pathsCustom                 atomic.Uint64
	numConnsDirect              atomic.Uint64
	numConnsOpened              atomic.Uint64
	numConnsClosed              atomic.Uint64
	transportIPPathsAdded       atomic.Uint64
	transportIPPathsRemoved     atomic.Uint64
	transportRelayPathsAdded    atomic.Uint64
	transportRelayPathsRemoved  atomic.Uint64
	transportCustomPathsAdded   atomic.Uint64
	transportCustomPathsRemoved atomic.Uint64
	actorLinkChange             atomic.Uint64
}

// MetricsSnapshot is a point-in-time copy of magic-socket counters.
type MetricsSnapshot struct {
	UpdateDirectAddrs           uint64
	IPv4Sent                    uint64
	IPv6Sent                    uint64
	RelaySent                   uint64
	CustomSent                  uint64
	EndpointIDSent              uint64
	Blackholed                  uint64
	IPv4Recv                    uint64
	IPv6Recv                    uint64
	RelayRecv                   uint64
	CustomRecv                  uint64
	RecvDatagrams               uint64
	RelayHomeChange             uint64
	RelayRateLimited            uint64
	HolepunchAttempts           uint64
	PathsDirect                 uint64
	PathsRelay                  uint64
	PathsCustom                 uint64
	NumConnsDirect              uint64
	NumConnsOpened              uint64
	NumConnsClosed              uint64
	TransportIPPathsAdded       uint64
	TransportIPPathsRemoved     uint64
	TransportRelayPathsAdded    uint64
	TransportRelayPathsRemoved  uint64
	TransportCustomPathsAdded   uint64
	TransportCustomPathsRemoved uint64
	ActorLinkChange             uint64
}

func (m *Metrics) snapshot() MetricsSnapshot {
	if m == nil {
		return MetricsSnapshot{}
	}
	return MetricsSnapshot{
		UpdateDirectAddrs:           m.updateDirectAddrs.Load(),
		IPv4Sent:                    m.ipv4Sent.Load(),
		IPv6Sent:                    m.ipv6Sent.Load(),
		RelaySent:                   m.relaySent.Load(),
		CustomSent:                  m.customSent.Load(),
		EndpointIDSent:              m.endpointIDSent.Load(),
		Blackholed:                  m.blackholed.Load(),
		IPv4Recv:                    m.ipv4Recv.Load(),
		IPv6Recv:                    m.ipv6Recv.Load(),
		RelayRecv:                   m.relayRecv.Load(),
		CustomRecv:                  m.customRecv.Load(),
		RecvDatagrams:               m.recvDatagrams.Load(),
		RelayHomeChange:             m.relayHomeChange.Load(),
		RelayRateLimited:            m.relayRateLimited.Load(),
		HolepunchAttempts:           m.holepunchAttempts.Load(),
		PathsDirect:                 m.pathsDirect.Load(),
		PathsRelay:                  m.pathsRelay.Load(),
		PathsCustom:                 m.pathsCustom.Load(),
		NumConnsDirect:              m.numConnsDirect.Load(),
		NumConnsOpened:              m.numConnsOpened.Load(),
		NumConnsClosed:              m.numConnsClosed.Load(),
		TransportIPPathsAdded:       m.transportIPPathsAdded.Load(),
		TransportIPPathsRemoved:     m.transportIPPathsRemoved.Load(),
		TransportRelayPathsAdded:    m.transportRelayPathsAdded.Load(),
		TransportRelayPathsRemoved:  m.transportRelayPathsRemoved.Load(),
		TransportCustomPathsAdded:   m.transportCustomPathsAdded.Load(),
		TransportCustomPathsRemoved: m.transportCustomPathsRemoved.Load(),
		ActorLinkChange:             m.actorLinkChange.Load(),
	}
}
