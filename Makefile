VERSION=HEAD-SNAPSHOT

.PHONY: build test release release-local

build:
	sbt +clean +compile

test:
	sbt +test

release:
	sbt 'set version := "$(VERSION)"' +publishSigned

snapshot:
	@echo No snapshot releases for this lib

# +publishLocal for your local .ivy2 repo
# +publishM2 for your local .m2 repo
release-local:
	sbt 'set version := "$(VERSION)"' +publishLocal +publishM2
