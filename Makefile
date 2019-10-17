
VERSION=HEAD-SNAPSHOT

.PHONY: build test release release-local

build:
	sbt +clean +compile

test:
	# Once we fix the issue with the tests on the Gorilla part, we should
	# replace this with sbt +test
	sbt test

release:
	sbt 'set version := "$(VERSION)"' +publishSigned

# Do a +publishLocal if you need it in your local .ivy2 repo
release-local:
	sbt 'set version := "$(VERSION)"' +publishM2
