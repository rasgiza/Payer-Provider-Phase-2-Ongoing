import requests
url = "https://raw.githubusercontent.com/rasgiza/Fabric-Payer-Provider-HealthCare-Demo/main/workspace/Healthcare_Analytics_Dashboard.pbix"
r = requests.head(url)
print(f"GitHub raw URL status: {r.status_code}")
print(f"Content-Length: {r.headers.get('Content-Length', '?')}")
print(f"Content-Type: {r.headers.get('Content-Type', '?')}")

# Also try full download to check size
r2 = requests.get(url)
print(f"Full download status: {r2.status_code}, size: {len(r2.content)} bytes")
