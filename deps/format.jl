try
    import JuliaFormatter
catch _error
    import Pkg
    Pkg.add("JuliaFormatter")
    import JuliaFormatter
end

JuliaFormatter.format(".")
