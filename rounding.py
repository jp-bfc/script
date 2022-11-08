a = range(0, 1000)

for n in a:
    #print()
    print("%s, %s" % (int(round((n%100)/49.0)*49.0) if round((n%100)/49.0) < 2 else int(round((n%100)/49.0)*49.0) +1, n))