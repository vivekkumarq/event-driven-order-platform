# Contributing

Thanks for taking a look. This is a portfolio project, but it is meant to be held to the same
standard as production code: if a change cannot be shown to work by a test, it is not finished.

## Getting set up

You need **JDK 17** and nothing else. Maven comes from the wrapper (`./mvnw`), the databases are
in-memory H2, and the integration tests start Kafka in-process via `@EmbeddedKafka`. Docker is only
needed if you want to run the whole stack together.

```bash
git clone https://github.com/<your-account>/event-driven-order-platform.git
cd event-driven-order-platform

cd order-service     && ./mvnw -B clean verify && cd ..
cd inventory-service && ./mvnw -B clean verify && cd ..
```

On Windows use `mvnw.cmd` in place of `./mvnw`.

## Ground rules

1. **Tests must pass, and they must not be skipped.** `./mvnw -B clean verify` has to be green in
   both modules. `-DskipTests` is for a Docker image build, never for a commit.
2. **New behaviour comes with a test that fails without it.** Unit tests for logic, MockMvc for
   controllers, `@EmbeddedKafka` for anything that crosses a topic.
3. **Nothing external in the test suite.** No Testcontainers, no broker on the developer's machine,
   no network. A contributor should be able to run everything on a plane.
4. **Document only what exists.** If the README claims an endpoint, a topic or a command, it has to
   be real and it has to work. An honest "not implemented yet" in the roadmap is worth more than an
   aspirational feature list.
5. **Both services stay independently deployable.** They deliberately keep their own copies of the
   event records and deserialise by explicit type rather than by Kafka type headers. Do not
   introduce a shared module that couples their deployments.

## Commit style

[Conventional Commits](https://www.conventionalcommits.org/). The scope is usually the module or the
concern:

```
feat(inventory): release reserved stock on order cancellation
fix(kafka): build producer config from spring.kafka properties
test(order): cover the cancelled-then-reserved compensation path
docs: document the event catalogue
```

Write the body for someone reading it in a year. Say what was wrong and why the change is right, not
what the diff already shows.

## Project layout

Each service is a standalone Spring Boot application with its own POM, so it can be built on its own
from inside its directory. The root POM is an aggregator that ties the two together; it is not their
parent.

```
order-service/       REST API, order lifecycle, transactional outbox
inventory-service/   stock domain, reservation saga participant, compensation
docker/              container support files
docs/                Postman collection, screenshots
```

## Things worth knowing before you change them

- **`OutboxService.append` is `@Transactional(MANDATORY)` on purpose.** Appending an event only
  means anything when it joins the caller's business transaction. If you relax it to `REQUIRED`, the
  dual-write problem the outbox exists to solve comes straight back.
- **`ProcessedEventEntity` and `StockReservationEntity` implement `Persistable`.** Their ids are
  assigned, not generated, so without this Spring Data would call `merge` and a concurrent duplicate
  would silently overwrite instead of colliding on the primary key. Removing it breaks idempotency
  in a way no single-threaded test will catch.
- **Retries live outside the transaction.** An optimistic locking conflict surfaces at commit, so it
  can only be caught by a caller outside the transaction boundary. That is why
  `ReservationCoordinator` is separate from `StockReservationService`.

## Troubleshooting

**`Selector.open()` fails with `Unable to establish loopback connection` on Windows.**
The JDK opens an internal pipe backed by a Unix-domain socket under `java.io.tmpdir`, and on some
Windows setups that temp path cannot be used for one. Every Kafka test then fails before it starts.
Point the JDK at a different directory:

```bash
./mvnw -B clean verify -DargLine="-Djdk.net.unixdomain.tmpdir=C:/Temp"
```

The directory has to exist. This is a machine-level quirk rather than a project one, which is why it
is not baked into the POM.
