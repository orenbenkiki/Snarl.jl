.DEFAULT: coverage

.PHONY: ci_build
ci_build: not_reached_lines

.PHONY: format
format:
	julia deps/format.jl

.PHONY: test
test: tracefile.info

tracefile.info: *.toml src/*.jl test/*.toml test/*.jl
	rm -f tracefile.info src/*.cov test/*.cov
	JULIA_NUM_THREADS=4 julia --code-coverage=tracefile.info deps/test.jl \
	|| (rm -f tracefile.info src/*.cov test/*.cov; false)

.PHONY: line_coverage
line_coverage: tracefile.info
	julia deps/line_coverage.jl 2>&1 | grep -v 'Info:'

.PHONY: not_reached_lines
not_reached_lines: tracefile.info
	deps/not_reached_lines.sh

.PHONY: coverage
coverage: not_reached_lines line_coverage

.PHONY: clean
clean:
	rm -f tracefile.info src/*.cov test/*.cov
