
def connect(*a,**k):
	import asyncserf
	return asyncserf.serf_client(*a,**k)
