import numpy as np
from .contracts import SPEED_KMH

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    to_rad = np.pi / 180.0
    dlat = (lat2 - lat1) * to_rad
    dlon = (lon2 - lon1) * to_rad
    a = np.sin(dlat/2)**2 + np.cos(lat1*to_rad)*np.cos(lat2*to_rad)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))

def transit_hours(km: float) -> float:
    return km / SPEED_KMH
