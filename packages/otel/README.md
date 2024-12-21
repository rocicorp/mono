# OTEL

To collect traces for local development:

1. Clone https://github.com/grafana/docker-otel-lgtm
2. `cd docker-otel-lgtm`
3. `./run-lgtm.sh`
4. Add `ZERO_LOG_TRACE_COLLECTOR = "http://localhost:4318/v1/traces"` to your `.env`
5. Open grafana at http://localhost:3000/
6. Go to explore
![CleanShot 2024-12-14 at 23 40 30@2x](https://github.com/user-attachments/assets/468df1f0-52cc-4e7f-ba8a-11c04aeb93f3)
8. Search tempo
![CleanShot 2024-12-14 at 23 40 46@2x](https://github.com/user-attachments/assets/1acfeee1-cffa-4914-841d-4bb9a3d02808)
