variable "CI" {
    // Set in GH actions to affect caching strategy
    default = false
}

variable "IMAGE_BASE" {
    default = "ghcr.io/matgenix/jobflow-remote-testing"
}

variable "IMAGE_TAG" {
    default = "latest"
}

group "default" {
    targets = [
        "slurm",
        "sge",
        "pbs"
    ]
}

target "slurm" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "slurm"
    }
    cache-from = [
        "type=registry,ref=${IMAGE_BASE}-slurm:${IMAGE_TAG}",
        "type=registry,ref=${IMAGE_BASE}-slurm:cache",
        "type=gha",
    ]
    // If in the CI, cache to GHA only, otherwise push to the registry cache
    cache-to = CI ? ["type=gha,mode=max"] : ["type=registry,ref=${IMAGE_BASE}-slurm:cache,mode=max"]
    tags = [
        "${IMAGE_BASE}-slurm:${IMAGE_TAG}",
    ]
    platforms = ["linux/amd64"]
}

target "sge" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "sge"
    }
    cache-from = [
        "type=registry,ref=${IMAGE_BASE}-sge:${IMAGE_TAG}",
        "type=registry,ref=${IMAGE_BASE}-sge:cache",
        "type=gha",
    ]
    cache-to = CI ? ["type=gha,mode=max"] : ["type=registry,ref=${IMAGE_BASE}-sge:cache,mode=max"]
    tags = [
        "${IMAGE_BASE}-sge:${IMAGE_TAG}",
    ]
    platforms = ["linux/amd64"]
}

target "pbs" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "pbs"
    }
    cache-from = [
        "type=registry,ref=${IMAGE_BASE}:${IMAGE_TAG}",
        "type=registry,ref=${IMAGE_BASE}-pbs:cache",
        "type=gha",
    ]
    cache-to = CI ? ["type=gha,mode=max"] : ["type=registry,ref=${IMAGE_BASE}-pbs:cache,mode=max"]
    tags = [
        "${IMAGE_BASE}-pbs:${IMAGE_TAG}",
    ]
    platforms = ["linux/amd64"]
}
