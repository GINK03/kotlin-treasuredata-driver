import glob
import os
import sys
import re
import concurrent.futures

def dealer(names):
  for name in names:
    with open(name) as f:
      head = next(f).strip()
      header = head.split(",")
      for i, line in enumerate(f):
        if i%10000 == 0:
          print("now iter %d"%i, file=sys.stderr)
        line = line.strip()
        o = dict(zip(header, line.split(",")))
        if o["最終リンク先URL"] != "--" and 'suumo.jp' in o["最終リンク先URL"]:
          print(o['日'], o['キーワード'], o["最終リンク先URL"])
print("start to globbing...", file=sys.stderr)          
names = [name for name in glob.glob("./yssDataset/*/*")]
print("finish to globbing...", file=sys.stderr)          
bucket = []
for i in range(0, len(names), 100):
  bucket.append( names[i:i+100] ) 

print("finish to building backet...", file=sys.stderr)          
with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
  [_ for _ in executor.map(dealer, bucket)]



