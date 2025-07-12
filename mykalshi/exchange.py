from .transport import kalshi_get

def get_exchange_announcements():
    return kalshi_get("/exchange/announcements")

def get_exchange_schedule():
    return kalshi_get("/exchange/schedule")

def get_exchange_status():
    return kalshi_get("/exchange/status")

def get_user_data_timestamp():
    return kalshi_get("/exchange/user_data_timestamp")
