.PHONY: pre_commit
pre_commit: _unindexed_files ci_build
	git status

.PHONY: ci_build
ci_build: format unindexed_files check coverage docs

.PHONY: _unindexed_files
_unindexed_files:
	@deps/unindexed_files.sh

.PHONY: unindexed_files
unindexed_files:
	@deps/unindexed_files.sh

.PHONY: format
format: deps/.formatted
deps/.formatted: */*.jl
	deps/format.sh
	@touch deps/.formatted

.PHONY: check
check: untested_lines

.PHONY: test
test: tracefile.info

tracefile.info: *.toml src/*.jl test/*.toml test/*.jl
	deps/test.sh

.PHONY: line_coverage
line_coverage: deps/.coverage

deps/.coverage: tracefile.info
	deps/line_coverage.sh
	@touch deps/.coverage

.PHONY: untested_lines
untested_lines: deps/.untested

deps/.untested: tracefile.info
	deps/untested_lines.sh
	@touch deps/.untested

.PHONY: coverage
coverage: untested_lines line_coverage

.PHONY: docs
docs: docs/build/index.html

docs/build/index.html: src/*.jl docs/src/*.md
	deps/document.sh

.PHONY: clean
clean:
	deps/clean.sh
