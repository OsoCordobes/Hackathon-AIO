import math
def haversine_km(lat1, lon1, lat2, lon2):
    if any(v is None for v in [lat1,lon1,lat2,lon2]): return None
    R=6371.0
    phi1,phi2=math.radians(lat1),math.radians(lat2)
    dphi=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
    a=math.sin(dphi/2)**2+math.cos(phi1)*math.cos(phi2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))
