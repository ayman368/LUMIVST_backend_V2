import urllib.request
import json
import ssl

def check_indicators():
    url = "http://localhost:8000/api/prices/history/1321?limit=1000"
    print(f"Fetching data from: {url}")
    
    try:
        # Avoid SSL issues for localhost if any
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ctx) as response:
            data = json.loads(response.read().decode())
            
            points = data.get("data", [])
            print(f"Total data points returned: {len(points)}")
            
            # Count how many points have RSI data
            rsi_points = [p for p in points if p.get("rsi_14") is not None]
            print(f"Points with RSI_14 data: {len(rsi_points)}")
            
            # Count how many points have THE.NUMBER data
            the_num_points = [p for p in points if p.get("the_number") is not None]
            print(f"Points with THE.NUMBER data: {len(the_num_points)}")
            
            # Print a sample of the most recent 3 points with RSI
            if rsi_points:
                print("\nSample (Last 3 points with RSI data):")
                for p in rsi_points[-3:]:
                    print(f"Date: {p.get('time')}")
                    print(f"  RSI_14: {p.get('rsi_14')}")
                    print(f"  CCI: {p.get('cci')}")
                    print(f"  CFG: {p.get('cfg')}")
                    print(f"  STAMP: {p.get('stamp_s9rsi')}")
                    print("-" * 30)
            else:
                print("\n⚠️ No indicators data found at all! This means the JOIN in the backend isn't returning data, or the stock_indicators table is empty for this symbol.")
                
            # Print the very last point total data just to see what we're receiving
            if points:
                print("\nVery last raw point (first 10 keys):")
                last_pt = points[-1]
                for k in list(last_pt.keys())[:10]:
                    print(f"  {k}: {last_pt.get(k)}")
                print("  ...")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    check_indicators()
