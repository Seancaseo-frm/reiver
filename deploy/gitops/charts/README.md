# Vendored Helm charts

- **drone** — Community [drone](https://github.com/community-charts/helm-charts/tree/main/charts/drone) chart (v0.1.4) with a fix for the NOTES template: `drone.fullname` is defined as an alias for `drone.serverFullname` so `helm template` (and Argo CD) succeed. Upstream NOTES.txt references `drone.fullname` but the chart only defined `drone.serverFullname` / `drone.runnerFullname`.
