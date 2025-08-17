package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JsonToAccess {


    // -------------------------------------------------------------------
    // 4️⃣  Main method – orchestrate everything
    // -------------------------------------------------------------------
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: java JsonToAccess <path-to-access-db> <path-to-json>");
            return;
        }

        String dbPath = args[0];
        String jsonPath = args[1];

        try (Connection conn = getConnection(dbPath)) {
            conn.setAutoCommit(false);   // batch transaction

            // 4.1  Parse JSON
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(new File(jsonPath));
            JsonNode appNode = root.get("applicationListDTO").get(0);

            // 4.2  Insert data
            insertApplication(conn, appNode);

            List<String> assessors = new ArrayList<>();
            for (JsonNode n : appNode.withArray("assessor")) assessors.add(n.asText());
            insertApplicationAssessors(conn, appNode.get("applicationId").asText(), assessors);

            List<String> states = new ArrayList<>();
            for (JsonNode n : appNode.withArray("memberStates")) states.add(n.asText());
            insertMemberStates(conn, appNode.get("applicationId").asText(), states);

            insertVehicles(conn,
                    appNode.get("applicationId").asText(),
                    appNode.get("vehicleIdentifier").asText());

            conn.commit();
            System.out.println("Import completed successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // -------------------------------------------------------------------
    // 1️⃣  JDBC connection – UCanAccess driver
    // -------------------------------------------------------------------
    private static Connection getConnection(String dbPath) throws SQLException {
        String url = "jdbc:ucanaccess://" + dbPath;
        return DriverManager.getConnection(url);
    }

    // -------------------------------------------------------------------
    // 2️⃣  Helper: INSERT into Applications
    // -------------------------------------------------------------------
    private static void insertApplication(Connection conn, JsonNode app) throws SQLException {
        String sql = "INSERT INTO Applications "
                + "(application_id, id, application_type_id, cached_last_update, decision_date, "
                + "project_name, submission, project_manager, assuror, decision_maker, "
                + "modified, application_type, application_type_variant_version, case_type, "
                + "completeness_acknowledgement, issuing_authority, ein, legal_denomination, "
                + "application_status, phase, subcategory, is_whole_eu, pre_engaged) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, app.get("applicationId").asText());
            ps.setString(2, app.get("id").asText());
            ps.setString(3, app.get("applicationTypeId").asText());
            ps.setTimestamp(4, Timestamp.valueOf(app.get("cachedLastUpdate").asText()));
            ps.setTimestamp(5, Timestamp.valueOf(app.get("decisionDate").asText()));
            ps.setString(6, app.get("projectName").asText());
            ps.setTimestamp(7, Timestamp.valueOf(app.get("submission").asText()));
            ps.setString(8, app.hasNonNull("projectManager") ? app.get("projectManager").asText() : null);
            ps.setString(9, app.hasNonNull("assuror") ? app.get("assuror").asText() : null);
            ps.setString(10, app.hasNonNull("decisionMaker") ? app.get("decisionMaker").asText() : null);
            ps.setTimestamp(11, Timestamp.valueOf(app.get("modified").asText()));
            ps.setString(12, app.get("applicationType").asText());
            ps.setString(13, app.get("applicationTypeVariantVersion").asText());
            ps.setString(14, app.get("caseType").asText());
            ps.setTimestamp(15, Timestamp.valueOf(app.get("completenessAcknowledgement").asText()));
            ps.setString(16, app.get("issuingAuthority").asText());
            ps.setString(17, app.hasNonNull("ein") ? app.get("ein").asText() : null);
            ps.setString(18, app.get("legalDenomination").asText());
            ps.setString(19, app.get("applicationStatus").asText());
            ps.setString(20, app.get("phase").asText());
            ps.setString(21, app.get("subcategory").asText());
            ps.setBoolean(22, app.get("isWholeEu").asBoolean());
            ps.setBoolean(23, app.get("preEngaged").asBoolean());
            ps.executeUpdate();
        }
    }

    // -------------------------------------------------------------------
    // 3️⃣  Helpers for many‑to‑many tables
    // -------------------------------------------------------------------
    private static long findOrCreateAssessor(Connection conn, String name) throws SQLException {
        // Try to fetch existing assessor_id
        String sel = "SELECT assessor_id FROM Assessors WHERE name = ?";
        try (PreparedStatement ps = conn.prepareStatement(sel)) {
            ps.setString(1, name);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getLong("assessor_id");
        }

        // Insert new assessor
        String ins = "INSERT INTO Assessors (name) VALUES (?)";
        try (PreparedStatement ps = conn.prepareStatement(ins, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, name);
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) return rs.getLong(1);
        }
        throw new SQLException("Could not create assessor: " + name);
    }

    private static void insertApplicationAssessors(Connection conn, String appId, List<String> assessors) throws SQLException {
        String ins = "INSERT INTO ApplicationAssessors (application_id, assessor_id) VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(ins)) {
            for (String name : assessors) {
                long aid = findOrCreateAssessor(conn, name);
                ps.setString(1, appId);
                ps.setLong(2, aid);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void insertMemberStates(Connection conn, String appId, List<String> states) throws SQLException {
        String ins = "INSERT INTO ApplicationMemberStates (application_id, state_code) VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(ins)) {
            for (String state : states) {
                ps.setString(1, appId);
                ps.setString(2, state);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static void insertVehicles(Connection conn, String appId, String vehicleList) throws SQLException {
        String[] ids = vehicleList.split(",");
        String ins = "INSERT INTO Vehicles (application_id, identifier) VALUES (?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(ins)) {
            for (String id : ids) {
                ps.setString(1, appId);
                ps.setString(2, id.trim());
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    
}