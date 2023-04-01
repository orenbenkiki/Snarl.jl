try
    import Documenter
catch _error
    import Pkg
    Pkg.add("Documenter")
    import Documenter
end

push!(LOAD_PATH, "../src/")

using Snarl

Documenter.makedocs(
    sitename = "Snarl.jl",
    modules = [Snarl],
    authors = "Oren Ben-Kiki",
    clean = true,
    format = Documenter.HTML(prettyurls = get(ENV, "CI", nothing) == "true"),
    pages = ["index.md", "setup.md", "storage.md", "control.md"],
)
