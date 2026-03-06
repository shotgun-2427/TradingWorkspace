# IBGW

This contains a docker-compose file to run IB Gateway locally. Create a `.env` file with the following environment variables:

```
TWS_USERID=
TWS_PASSWORD=
TRADING_MODE=paper
READ_ONLY_API=no
TWOFA_TIMEOUT_ACTION=restart
RELOGIN_AFTER_TWOFA_TIMEOUT=yes
EXISTING_SESSION_DETECTED_ACTION=primary
```

Change these settings as needed for your use case.

