export class ZipLocksDO {
  state: DurableObjectState;
  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(req: Request) {
    const url = new URL(req.url);

    if (url.pathname === "/lock") {
      console.log(`Locking object ${this.state.id} ...`);

      // Acquire a short lock if not already locked
      const existing = await this.state.storage.getAlarm();
      if (existing) {
        console.log(`Object Locked: ${this.state.id}`);
        return new Response("locked", { status: 423 });
      }

      // set a short TTL alarm (e.g., 2 minutes)
      await this.state.storage.setAlarm(Date.now() + 120_000);
      console.log(`Object Locked: ${this.state.id}`);

      return new Response("ok");
    }

    if (url.pathname === "/unlock") {
      console.log(`Unlocking object ${this.state.id} ...`);
      await this.state.storage.deleteAlarm();
      console.log(`Object unlocked: ${this.state.id}.`);
      return new Response("ok");
    }

    return new Response("not found", { status: 404 });
  }

  async alarm() {
    // Lock expires automatically when alarm fires
    console.log("Object automatically unlocked:", this.state.id);
    await this.state.storage.deleteAlarm();
  }
}
