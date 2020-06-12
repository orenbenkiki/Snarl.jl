try
    using JuliaFormatter
catch error
    if isa(error, LoadError)
        using Pkg
        Pkg.add("JuliaFormatter")
        using JuliaFormatter
    else
        rethrow()
    end
end

import JuliaFormatter
JuliaFormatter.format(".")
