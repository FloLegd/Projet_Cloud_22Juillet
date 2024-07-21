from azure.identity import DefaultAzureCredential


def get_credential() -> DefaultAzureCredential:
    """Get Azure credential for DataLakeGen2 storage."""
    return DefaultAzureCredential(
        exclude_environment_credential=True,
        exclude_cli_credential=False,
        exclude_managed_identity_credential=False,
        exclude_powershell_credential=True,
        exclude_shared_token_cache_credential=True,
        exclude_interactive_browser_credential=True,
    )
