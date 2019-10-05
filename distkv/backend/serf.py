import asyncserf

# Simply setting connect=asyncserf.serf_client interferes with mocking
# when testing.

def connect(*a,**k):
	return asyncserf.serf_client(*a,**k)
