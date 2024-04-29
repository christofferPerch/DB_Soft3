from flask import Flask, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "passwordss"))

def get_query_result(query, parameters=None):
    with driver.session() as session:
        result = session.run(query, parameters)
        return [record.data() for record in result]

@app.route("/organizations")
def list_organizations():
    query = """
    MATCH (o:Organization)-[:LOCATED_IN]->(c:Country)
    RETURN o.name AS Organization, c.name AS Country
    """
    return jsonify(get_query_result(query))

@app.route("/organizations/emissions")
def organization_emissions():
    year = request.args.get('year')
    if year is not None:
        year = int(year)  
    else:
        return jsonify({"error": "Year parameter is required"}), 400
    
    query = """
    MATCH (o:Organization)-[:REPORTED]->(e:EmissionResult {year: $year})
    RETURN o.name AS Organization, e.totalEmissions AS EmissionAmount
    """
    return jsonify(get_query_result(query, parameters={'year': year}))

@app.route("/organizations/gases")
def organizations_using_gas():
    gas_name = request.args.get('gas')
    query = """
    MATCH (o:Organization)-[:USES_GAS]->(g:Gas {name: $gas_name})
    RETURN o.name AS Organization
    """
    return jsonify(get_query_result(query, parameters={'gas_name': gas_name}))

@app.route("/organizations/population")
def organization_population():
    year = request.args.get('year')
    if year is not None:
        year = int(year)  
    else:
        return jsonify({"error": "Year parameter is required"}), 400
    
    query = """
    MATCH (o:Organization)-[:HAS_POPULATION]->(p:Population {year: $year})
    RETURN o.name AS Organization, p.currentPopulation AS PopulationCount
    """
    return jsonify(get_query_result(query, parameters={'year': year}))


@app.route("/organizations/gdp")
def organization_gdp():
    year = request.args.get('year')
    if year is not None:
        year = int(year)  
    else:
        return jsonify({"error": "Year parameter is required"}), 400
    
    query = """
    MATCH (o:Organization)-[:HAS_GDP]->(g:GDP {year: $year})
    RETURN o.name AS Organization, g.value AS GDPValue
    """
    return jsonify(get_query_result(query, parameters={'year': year}))

@app.route("/organizations/targets")
def emission_reduction_targets():
    query = """
    MATCH (o:Organization)-[:HAS_TARGET]->(t:TargetEmission)
    RETURN o.name AS Organization, t.baselineEmissions AS TargetEmission
    """
    return jsonify(get_query_result(query))

@app.route("/organizations/c40")
def list_c40_cities():
    query = """
    MATCH (o:Organization {C40: 1})
    RETURN o.name AS Organization
    """
    return jsonify(get_query_result(query))

@app.route("/organizations/gases/used")
def gases_used_by_organization():
    organization_name = request.args.get('organization')
    if not organization_name:
        return jsonify({'error': 'Organization name is required'}), 400
    query = """
    MATCH (o:Organization {name: $organization_name})-[:USES_GAS]->(g:Gas)
    RETURN g.name AS Gas
    """
    return jsonify(get_query_result(query, parameters={'organization_name': organization_name}))

@app.route("/organizations/methodologies")
def methodologies_used_by_organizations():
    query = """
    MATCH (o:Organization)-[:USES_METHOD]->(m:Methodology)
    RETURN o.name AS Organization, m.primaryMethodology AS MethodologyDescription
    """
    return jsonify(get_query_result(query))

@app.route("/countries/most-organizations")
def countries_with_most_organizations():
    query = """
    MATCH (o:Organization)-[:LOCATED_IN]->(c:Country)
    WITH c, COUNT(o) AS orgCount
    ORDER BY orgCount DESC
    LIMIT 3
    RETURN c.name AS Country, orgCount AS NumberOfOrganizations
    """
    return jsonify(get_query_result(query))


if __name__ == "__main__":
    app.run(debug=True, port=5001)
