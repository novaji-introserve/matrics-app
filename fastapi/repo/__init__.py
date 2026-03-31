"""Repository package.

Keep package imports side-effect free so submodules such as repo.fraud.predict
can be imported without pulling in unrelated integrations.
"""
