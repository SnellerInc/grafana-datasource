import { DataSourceVariableSupport } from "@grafana/data";
import { DataSource } from "./datasource";
import { SnellerQuery } from "./types";

export class SnellerVariableSupport extends DataSourceVariableSupport<DataSource, SnellerQuery> {
}
