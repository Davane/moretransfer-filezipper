export class MissingMultipartPartsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MissingMultipartPartsError";
  }
}

export class ContainerRunError extends Error {
  constructor(
    public readonly status: number,
    public readonly errText: string,
  ) {
    super(`Container run failed: ${status} ${errText}`);
    this.name = "ContainerRunError";
  }
}
