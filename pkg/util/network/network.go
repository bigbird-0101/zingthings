package network

import "net"

func GetLocalIP() (string, error) {
	ps, err := getMachineIPs()
	if err != nil {
		return "", err
	}
	return ps[0], nil
}

func getMachineIPs() ([]string, error) {
	var ips []string
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // 接口未启用
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // 环回接口
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // 这是一个IPv6地址
			}
			ips = append(ips, ip.String())
		}
	}

	return ips, nil
}
