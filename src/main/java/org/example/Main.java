package org.example;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        // Abra a DB no Docker:
        // docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb

        // remote indica que é um servidor standalone : localhost pode ser substituído pelo IP/URL do host.
        OrientDB orient = new OrientDB("remote:localhost", "root", "rootpwd", OrientDBConfig.defaultConfig());
        // orient.createIfNotExists("test", ODatabaseType.PLOCAL);
        // infelizmente, a documentação diz que a db cria um usuário admin com senha admin automaticamente
        // mas isto não está ocorrendo. acessar http://localhost:2480 e criar db manualmente
        ODatabaseSession db = orient.open("test", "admin", "admin");

        createSchema(db);

        createPeople(db);

        executeAQuery(db);

        executeAnotherQuery(db);

        db.close();
        orient.close();

    }

    private static void createSchema(ODatabaseSession db) {
        OClass person = db.getClass("Person");

        if (person == null) {
            person = db.createVertexClass("Person");
        }

        if (person.getProperty("name") == null) {
            person.createProperty("name", OType.STRING);
            person.createIndex("Person_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "name");
        }

        if (db.getClass("FriendOf") == null) {
            db.createEdgeClass("FriendOf");
        }

    }

    private static void createPeople(ODatabaseSession db) {
        OVertex alice = createPerson(db, "Alice", "Foo");
        OVertex bob = createPerson(db, "Bob", "Bar");
        OVertex jim = createPerson(db, "Jim", "Baz");

        OEdge edge1 = alice.addEdge(bob, "FriendOf");
        edge1.save();
        OEdge edge2 = bob.addEdge(jim, "FriendOf");
        edge2.save();
    }

    private static OVertex createPerson(ODatabaseSession db, String name, String surname) {
        OVertex result = db.newVertex("Person");
        result.setProperty("name", name);
        result.setProperty("surname", surname);
        result.save();
        return result;
    }

    private static void executeAQuery(ODatabaseSession db) {
        String query = "SELECT expand(out('FriendOf').out('FriendOf')) from Person where name = ?";
        OResultSet rs = db.query(query, "Alice");

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("Amigos de amigos de Alice: " + item.getProperty("name") + " " + item.getProperty("surname"));
        }

        rs.close();
    }

    private static void executeAnotherQuery(ODatabaseSession db) {
        // retorna pessoas que são amigas tanto de Jim quanto Alice
        String query =
                " MATCH                                           " +
                        "   {class:Person, as:a, where: (name = :name1)}, " +
                        "   {class:Person, as:b, where: (name = :name2)}, " +
                        "   {as:a} -FriendOf-> {as:x} -FriendOf-> {as:b}  " +
                        " RETURN x.name as friend                         ";

        Map<String, Object> params = new HashMap<String, Object>();
        params.put("name1", "Alice");
        params.put("name2", "Jim");

        OResultSet rs = db.query(query, params);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println("Amigo de Jim e Alice: " + item.getProperty("friend"));
        }

        rs.close();
    }

}