package server

import "net"

// httpDashboardURLs возвращает URL дашборда для localhost и (если есть) IP WSL/ЛAN.
// В WSL2 браузер Windows часто не видит 127.0.0.1 сервиса внутри Linux — нужен IP из hostname -I.
func httpDashboardURLs(httpAddr string) (localhost, lan string) {
	localhost = httpListenURL(httpAddr) + "/dashboard/"

	_, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return localhost, ""
	}
	if ip := firstNonLoopbackIPv4(); ip != "" {
		lan = "http://" + net.JoinHostPort(ip, port) + "/dashboard/"
	}
	return localhost, lan
}

func firstNonLoopbackIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.To4() == nil {
			continue
		}
		return ipNet.IP.String()
	}
	return ""
}
