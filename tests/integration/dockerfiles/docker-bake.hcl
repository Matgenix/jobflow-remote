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
    tags = [
        "jobflow-remote-testing-slurm:latest"
    ]
    platforms = ["linux/amd64"]
}

target "sge" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "sge"
    }
    tags = [
        "jobflow-remote-testing-sge:latest"
    ]
    platforms = ["linux/amd64"]
}

target "pbs" {
    dockerfile = "./tests/integration/dockerfiles/Dockerfile"
    args = {
        QUEUE_SYSTEM = "pbs"
    }
    tags = [
        "jobflow-remote-testing-pbs:latest"
    ]
    platforms = ["linux/amd64"]
}
