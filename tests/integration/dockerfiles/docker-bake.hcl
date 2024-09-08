group "default" {
    targets = [
        "slurm",
        "sge"
    ]
}

target "slurm" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile.slurm"
    args = {
        QUEUE_SYSTEM = "slurm"
    }
    tags = [
        "jobflow-remote-testing-slurm:latest"
    ]
}

target "sge" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile.sge"
    args = {
        QUEUE_SYSTEM = "sge"
    }
    tags = [
        "jobflow-remote-testing-sge:latest"
    ]
}
