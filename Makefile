#!/usr/bin/make -f

.PHONY: doc test update all tag pypi upload

install:
	mkdir -p $(PREFIX)/lib/systemd/system
	mkdir -p $(PREFIX)/usr/bin
	mkdir -p $(PREFIX)/usr/lib/distkv
	mkdir -p $(PREFIX)/usr/lib/sysusers.d
	cp systemd/*.service $(PREFIX)/lib/systemd/system/
	cp systemd/*.timer $(PREFIX)/lib/systemd/system/
	cp systemd/sysusers $(PREFIX)/usr/lib/sysusers.d/distkv.conf
	cp scripts/* $(PREFIX)/usr/lib/distkv/
	cp bin/* $(PREFIX)/usr/bin/

PACKAGE=distkv
ifneq ($(wildcard /usr/share/sourcemgr/make/py),)
include /usr/share/sourcemgr/make/py
# availabe via http://github.com/smurfix/sourcemgr

else
%:
		@echo "Please use 'python setup.py'."
		@exit 1
endif
