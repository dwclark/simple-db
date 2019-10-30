package simpledb.server

class Config {
    final String databaseDirectory

    private Config(final Builder builder) {
        databaseDirectory = builder.databaseDirectory
    }
    
    static class Builder {
        String databaseDirectory = System.properties['simple.db.dir']

        Config config() {
            return new Config(this)
        }
    }
}
