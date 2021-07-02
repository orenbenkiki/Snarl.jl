using Documenter

push!(LOAD_PATH, "src/")

using Snarl

makedocs(
    sitename = "Snarl.jl",
    modules = [Snarl],
    authors = "Oren Ben-Kiki",
    format = Documenter.HTML(prettyurls = get(ENV, "CI", nothing) == "true"),
    pages = ["index.md", "setup.md", "storage.md", "control.md"],
)
