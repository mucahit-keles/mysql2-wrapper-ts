import { Connection, ConnectionOptions, createConnection, ResultSetHeader, RowDataPacket } from "mysql2/promise";

let dbConnection: Connection;

export async function getWrapper(connectionOptions: ConnectionOptions, timeZone?: string): Promise<IWrapper | undefined> {
	if (!dbConnection) {
		dbConnection = await createConnection(connectionOptions);
		if (timeZone) await dbConnection.execute("SET `time_zone` = ?", [ timeZone ]);
	}
	return new IWrapper();
}

type IQueryValue = string | number | Date;
type IDBColumn = { Field: string };
type ICountValue = { count: number };

export class IWrapper {
	public async getData(query: string, params?: IQueryValue[]): Promise<RowDataPacket[]> {
		try {
			return (await dbConnection.execute(query, params))[0] as RowDataPacket[];
		} catch (e: any) {
			throw new Error(`getData Exception: ${e.message}`);
		}
	};
	public async getOneData(query: string, params?: IQueryValue[]): Promise<RowDataPacket | undefined> {
		try {
			const rows: RowDataPacket[] = await this.getData(`${query} LIMIT 1`, params);
			return rows.length === 0 ? undefined : rows[0];
		} catch (e: any) {
			throw new Error(`getOneData Exception: ${e.message.replace("getData Exception: ", "")}`);
		}
	};
	public async getColumns(tableName: string, excludedColumns?: string[]): Promise<string[]> {
		let excludedColumns_params: Array<"?"> | undefined = excludedColumns ? new Array(excludedColumns.length).fill("?") : undefined;
		let excludedColumns_params_csv: string | undefined = excludedColumns_params ? excludedColumns_params.join(",") : undefined;
		
		const columns_queryParams: string = excludedColumns_params_csv ? ` WHERE \`Field\` NOT IN (${excludedColumns_params_csv})` : "";
		const columns: IDBColumn[] = await this.getData(`SHOW COLUMNS IN \`${tableName}\`${columns_queryParams}`, excludedColumns) as IDBColumn[];
		
		return columns.map((column: IDBColumn): string => column.Field) as string[];
	};
	public async execute(query: string, params?: IQueryValue): Promise<number> {
		try {
			return ((await dbConnection.execute(query, params))[0] as ResultSetHeader).insertId;
		} catch (e: any) {
			throw new Error(`execute Exception: ${e.message}`)
		}
	};
}
