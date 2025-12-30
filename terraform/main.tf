terraform {
  required_providers {
    github = {
      source  = "integrations/github"
      version = "~> 6.0"
    }
  }
}

# Configure the GitHub Provider
provider "github" {
  token = var.github_token
}

# Define the Public Repository
resource "github_repository" "my_public_repo" {
  name        = "hyperion"
  description = "PROJECT HYPERION: The Infinite Stream Architect"

  # Setting visibility to "public" makes it accessible to everyone
  visibility = "public"

  # Optional: Initialize with a Readme, License, and .gitignore
  auto_init          = true
  license_template   = "mit"
  gitignore_template = "Terraform"
}