# AwsIO.jl

Wrapper library for the https://github.com/awslabs/aws-c-io library.

The `LibAwsIO` module (exported) aims to directly wrap and expose aws-c-io functionality (matching
data structures and api functions exactly).

The functions and structures in `AwsIO` are more Julia-like and are intended to be more user-friendly,
while using `LibAwsIO` under the hood.

GitHub Actions : [![Build Status](https://github.com/JuliaServices/AwsIO.jl/workflows/CI/badge.svg)](https://github.com/JuliaServices/AwsIO.jl/actions?query=workflow%3ACI+branch%3Amaster)

[![codecov.io](http://codecov.io/github/JuliaServices/AwsIO.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaServices/AwsIO.jl?branch=master)
