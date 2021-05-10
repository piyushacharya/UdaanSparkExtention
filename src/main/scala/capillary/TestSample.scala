package capillary

class TestSample {

}


class GraphLoader {


  def getGraph (): Graph =
  {
    val graph = new Graph();
    val n1 = new Job_node("n1",null,null,"CREATE TABLE p_country (id STRING, country STRING) USING CSV LOCATION '/Users/piyush.acharya/MyWorkSpace/tempSpace/data/country'")
    val n2 = new Job_node("n2",null,null,"CREATE TABLE p_name (id STRING, name STRING) USING CSV LOCATION '/Users/piyush.acharya/MyWorkSpace/tempSpace/data/name.csv'")
    val n3 = new Job_node("n3",null,null,"CREATE TABLE p_orgn (id STRING, orgn STRING) USING CSV LOCATION '/Users/piyush.acharya/MyWorkSpace/tempSpace/data/orgn'")

    val n4 = new Job_node("n4",null,null,"create table orgn_country using delta as select p_orgn.id, orgn , country from p_orgn join p_country as p_country on (p_orgn.id = p_country.id )")

    val n5 = new Job_node("n5",null,null,"CREATE OR REPLACE VIEW  temp_name_view as select orgn_country.id,orgn,country,name  from p_name join orgn_country_fake  on (orgn_country.id = p_name.id)")
    val n6 = new Job_node("n6",null,null,"create table final_table  using delta as select * from temp_name_view")

    val n7 = new Job_node("n7",null,null,"create or replace  table country_fact using delta as  select count(*) as c_count, country  from final_table group by country")
    val n8 = new Job_node("n8",null,null,"create or replace  table name_fact using delta as select count(*) as n_count, name  from final_table group by name")
    val n9 = new Job_node("n9",null,null,"create or replace  table orgn_fact using delta as select count(*) as o_count, orgn  from final_table group by orgn")


    graph.add_node(n1)
    graph.add_node(n2)
    graph.add_node(n3)

    graph.add_node(n4)
    graph.add_node(n5)
    graph.add_node(n6)
    graph.add_node(n7)
    graph.add_node(n8)
    graph.add_node(n9)


    graph.add_Edge(n1,n4)
    graph.add_Edge(n3,n4)

    graph.add_Edge(n2,n5)
    graph.add_Edge(n4,n5)

    graph.add_Edge(n5,n6)

    graph.add_Edge(n6,n7)
    graph.add_Edge(n6,n8)
    graph.add_Edge(n6,n9)

    return graph;

  }


}
