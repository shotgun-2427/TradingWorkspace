from ib_async import IB

ports = [4004]

for port in ports:
    ib = IB()
    try:
        print(f"Testing {port}...")
        ib.connect("127.0.0.1", port, clientId=999, timeout=5)
        print(f"CONNECTED: {port}")
        print("Managed accounts:", ib.managedAccounts())
        print("Account summary rows:", len(ib.accountSummary()))
        ib.disconnect()
    except Exception as exc:
        print(f"FAILED: {port} -> {repr(exc)}")