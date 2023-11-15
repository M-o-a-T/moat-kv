PACKAGE = moat-kv
MAKEINCL ?= $(shell python3 -mmoat src path)/make/py

install:
	mkdir -p $(PREFIX)/lib/systemd/system
	mkdir -p $(PREFIX)/usr/lib/moat/kv
	mkdir -p $(PREFIX)/usr/lib/sysusers.d
	cp systemd/*.service $(PREFIX)/lib/systemd/system/
	cp systemd/*.timer $(PREFIX)/lib/systemd/system/
	cp systemd/sysusers $(PREFIX)/usr/lib/sysusers.d/moat-kv.conf
	cp scripts/* $(PREFIX)/usr/lib/moat/kv/

ifneq ($(wildcard $(MAKEINCL)),)
include $(MAKEINCL)

else
%:
	@echo "Please fix 'python3 -mmoat src path'."
	@exit 1
endif

