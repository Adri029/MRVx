Build and run the workers:
- Install Java and Maven;
- `cd mrvgenericworker`;
- Edit the `src/main/resources/config.yml` accordingly;
- Compile: `mvn clean install`;
- Run: `mvn exec:java -Dexec.mainClass="Main"`;