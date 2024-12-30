group "default" {
    targets = [
        "slurm",
        "sge"
    ]
}

target "slurm" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "slurm"
    }
    tags = [
        "jobflow-remote-testing-slurm:latest"
    ]
}

target "sge" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "sge"
    }
    tags = [
        "jobflow-remote-testing-sge:latest"
    ]
}

target "pbs" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "pbs"
    }
    tags = [
        "jobflow-remote-testing-pbs:latest"
    ]
}
