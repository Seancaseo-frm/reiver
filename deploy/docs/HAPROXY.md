# HAProxy Load Balancer

L4 TCP load balancer on a Hetzner Cloud VPS, forwarding traffic to the k3s
production nodes running Traefik.

## Architecture

```
Client :443/:80
   |
   v
HAProxy VPS (YOUR_SERVER_IP)  — L4 TCP passthrough, no TLS termination
   |  round-robin
   +--> prod-1  YOUR_SERVER_IP:31871/31709
   +--> prod-2  YOUR_SERVER_IP:31871/31709
   +--> prod-3  37.27.58.55:31871/31709
                   ^
                   Traefik NodePorts (TLS termination + L7 routing)
```

HAProxy operates in pure TCP mode. TLS termination, certificate management, and
HTTP routing are all handled by Traefik inside the cluster.

## VPS Details

| Field | Value |
|-------|-------|
| Provider | Hetzner Cloud |
| Type | CX22 |
| OS | Ubuntu 24.10 (Resolute) |
| IP | YOUR_SERVER_IP |
| HAProxy version | 3.2.9 |
| Config path | `/etc/haproxy/haproxy.cfg` |

## Ports

| Port | Purpose | Access |
|------|---------|--------|
| 80 | HTTP passthrough to Traefik | Public |
| 443 | HTTPS passthrough to Traefik | Public |
| 8404 | HAProxy stats dashboard | Restricted to admin IP |
| 22 | SSH | Restricted to admin IP |

## Stats Dashboard

Available at `http://YOUR_SERVER_IP:8404/stats` (requires basic auth).
Shows real-time status of all backends, connection counts, health check state,
bytes in/out, and response times.

## Backend NodePorts

Traefik is exposed via k3s `LoadBalancer` service with these NodePorts:

| Protocol | NodePort |
|----------|----------|
| HTTP | 31709 |
| HTTPS | 31871 |

These are assigned by k3s and are stable unless the Traefik service is
recreated. If they change, update `/etc/haproxy/haproxy.cfg` on the VPS.

To check current NodePorts:

```bash
kubectl get svc -n kube-system traefik -o jsonpath='{.spec.ports[*].nodePort}'
```

## Common Operations

### Add a new node

Edit `/etc/haproxy/haproxy.cfg` on the VPS, adding a `server` line to both
the `k8s_https` and `k8s_http` backends:

```
server prod-N <node-ip>:31871 check inter 5s fall 3 rise 2
```

Then reload:

```bash
ssh root@YOUR_SERVER_IP "haproxy -c -f /etc/haproxy/haproxy.cfg && systemctl reload haproxy"
```

### Remove a node

Delete the corresponding `server` lines from both backends, then reload.

### Drain a node

Temporarily disable a backend server without editing config:

```bash
ssh root@YOUR_SERVER_IP "echo 'disable server k8s_https/prod-N' | socat stdio /run/haproxy/admin.sock"
ssh root@YOUR_SERVER_IP "echo 'disable server k8s_http/prod-N' | socat stdio /run/haproxy/admin.sock"
```

Re-enable:

```bash
ssh root@YOUR_SERVER_IP "echo 'enable server k8s_https/prod-N' | socat stdio /run/haproxy/admin.sock"
ssh root@YOUR_SERVER_IP "echo 'enable server k8s_http/prod-N' | socat stdio /run/haproxy/admin.sock"
```

### Check status from CLI

```bash
ssh root@YOUR_SERVER_IP "echo 'show stat' | socat stdio /run/haproxy/admin.sock | cut -d, -f1,2,18 | column -t -s,"
```

### Update firewall for new admin IP

```bash
ssh root@YOUR_SERVER_IP "ufw allow from <new-ip> to any port 8404 && ufw allow from <new-ip> to any port 22"
```

## Firewall (UFW)

The VPS runs UFW with default-deny incoming. Rules:

- 80/tcp, 443/tcp: open to all (public traffic)
- 8404, 22: restricted to admin IP(s)

## HA (future)

To eliminate the VPS as a single point of failure:

1. Create a second CX22 VPS with the same HAProxy config
2. Install Keepalived on both, using VRRP for leader election
3. Allocate a Hetzner Floating IP
4. Configure a Keepalived notify script to reassign the Floating IP via the
   Hetzner API on failover
5. Point DNS A records at the Floating IP instead of the VPS IP
