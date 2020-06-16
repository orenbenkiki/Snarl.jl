import (Pkg)
Pkg.activate(".")
Pkg.test(coverage = true, test_args = Base.ARGS)
