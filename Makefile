.PHONY: check
check: unreached_lines

.PHONY: ci_build
ci_build: format unindexed_files check

.PHONY: pre_commit
pre_commit: _unindexed_files ci_build line_coverage
	git status

.PHONY: _unindexed_files
_unindexed_files:
	@deps/unindexed_files.sh

.PHONY: unindexed_files
unindexed_files:
	deps/unindexed_files.sh

.PHONY: format
format: deps/.formatted
deps/.formatted: */*.jl
	deps/format.sh
	@touch deps/.formatted

.PHONY: test
test: tracefile.info

tracefile.info: *.toml src/*.jl test/*.toml test/*.jl
	deps/test.sh

.PHONY: line_coverage
line_coverage: tracefile.info
	deps/line_coverage.sh

.PHONY: unreached_lines
unreached_lines: tracefile.info
	deps/unreached_lines.sh

.PHONY: coverage
coverage: unreached_lines line_coverage

.PHONY: clean
clean:
	deps/clean.sh
