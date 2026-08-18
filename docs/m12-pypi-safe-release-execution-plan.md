# M12 - PyPI-safe release

Publish a standard wheel and source distribution while retaining private
development and reference-cluster material in the GitHub repository.

1. Use the pure-Python `uv_build` backend to restrict artifacts to the package,
   a sanitized PyPI README, metadata, and Apache-2.0 license.
2. Replace reference-cluster constants in shipped code with generic,
   target-derived policy.
3. Audit archive paths and text for infrastructure, identity, result, and
   credential markers.
4. Build and validate once, publish to TestPyPI, then publish the same artifacts
   through a protected PyPI OIDC environment.

GitHub-only documentation, examples, tests, plans, and system evidence remain
tracked but are never selected by the package build.
