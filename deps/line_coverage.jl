using Coverage
# Process '*.cov' files
coverage = process_folder() # defaults to src/; alternatively, supply the folder name as argument
coverage = append!(coverage, process_folder("test"))
# Process '*.info' files
coverage = merge_coverage_counts(
    coverage,
    filter!(
        let prefixes = (joinpath(pwd(), "src", ""), joinpath(pwd(), "test", ""))
            c -> any(p -> startswith(c.filename, p), prefixes)
        end,
        LCOV.readfolder("test"),
    ),
)
# Get total coverage for all Julia files
covered_lines, total_lines = get_summary(coverage)
percent = round(Int, 100 * covered_lines / total_lines)
println("Line coverage: $(percent)% ($(covered_lines) covered out of $(total_lines) total lines)")
