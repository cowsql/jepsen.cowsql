#!/usr/bin/env bash

# Run a Jepsen test locally.
#
# Usage:
#
# ./test.sh setup
# ./test.sh run JEPSEN_ARGS
#
# `setup` should be run only once to launch and initialize the jepsen container.
# JEPSEN_ARGS are passed directly to Jepsen (you can omit --no-ssh and --binary).
# For `./test.sh run`, set the env vars RAFT_BRANCH and COWSQL_BRANCH to control
# which raft/cowsql are built, e.g. RAFT_BRANCH=mathieu/fix will build the `fix`
# branch of https://github.com/MathieuBordere/raft.

set -o errexit -o pipefail -o nounset

jepsen=jepsen
workspace=/root/workspace

push-this-repo() {
	parent="$(dirname "${BASH_SOURCE[0]}")"
	lxc file push -p -r "$parent"/* "$jepsen$workspace/jepsen.cowsql"
}

setup-inner() {
	echo Set disable_coredump false >>/etc/sudo.conf
	echo Defaults rlimit_core=infinity >/etc/sudoers.d/99-core

	apt update
	apt install -y \
		autoconf \
		build-essential \
		git \
		gnuplot \
		graphviz \
		iptables \
		leiningen \
		libjna-java \
		liblz4-dev \
		libsqlite3-dev \
		libtool \
		libuv1-dev \
		make \
		pkg-config \
		snapd
	snap install go --classic

	pushd "$workspace"

	git clone -o cowsql https://github.com/cowsql/raft
	pushd raft
	git remote add cole https://github.com/cole-miller/raft
	git remote add mathieu https://github.com/MathieuBordere/raft
	git remote add mohamed https://github.com/mwnsiri/raft
	popd

	git clone -o cowsql https://github.com/cowsql/cowsql
	pushd cowsql
	git remote add cole https://github.com/cole-miller/cowsql
	git remote add mathieu https://github.com/MathieuBordere/cowsql
	git remote add mohamed https://github.com/mwnsiri/cowsql
	popd

	popd
}

setup() {
	lxc launch images:ubuntu/22.04 jepsen -c limits.kernel.core=-1
	sleep 5
	push-this-repo
	lxc exec "$jepsen" -- "$workspace/jepsen.cowsql/test.sh" setup-inner "$@"
}

run-inner() {
	pushd "$workspace"

	pushd raft
	git fetch --all
	git checkout "$RAFT_BRANCH"
	autoreconf -i
	./configure --enable-debug
	make -j"$(nproc)"
	make install
	popd

	pushd cowsql
	git fetch --all
	git checkout "$COWSQL_BRANCH"
	autoreconf -i
	./configure --enable-debug
	make -j"$(nproc)"
	make install
	popd

	pushd jepsen.cowsql
	ip link show jepsen-br >/dev/null 2>&1 || resources/network.sh setup 5
	go get golang.org/x/sync/semaphore
	go get -tags libsqlite3 github.com/cowsql/go-cowsql/app
	ldconfig
	CGO_LDFLAGS_ALLOW='-Wl,-z,now' go build -tags libsqlite3 -o resources/app resources/app.go
	lein run test --no-ssh --binary "$(pwd)/resources/app" "$@"
	popd

	popd
}

run() {
	test "$(sysctl -n kernel.core_pattern)" = core || exit 1
	test "$(sysctl -n fs.suid_dumpable)" -gt 0 || exit 1
	push-this-repo
	lxc exec $jepsen -- \
		env RAFT_BRANCH="${RAFT_BRANCH:-cowsql/master}" \
		    COWSQL_BRANCH="${COWSQL_BRANCH:-cowsql/master}" \
		    "$workspace/jepsen.cowsql/test.sh" run-inner "$@"
}

"$@"
