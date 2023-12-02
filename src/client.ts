import { nanoid } from 'nanoid';
import type {
	CallbackUserFunction,
	ProcessorConfig,
	ConfigMessage,
	DestroyMessage,
	FunctionMessage,
	OmitQueryKey,
	OutputMessage,
	QueryKey,
	QueryMessage,
	Sqlite3Method,
	TransactionMessage,
} from './types';
import { SQLocalProcessor } from './processor';

export class SQLocal {
	protected databasePath: string | undefined;
	protected processor: SQLocalProcessor | Worker;
	protected isDestroyed: boolean = false;
	protected userCallbacks = new Map<string, CallbackUserFunction['handler']>();
	protected queriesInProgress = new Map<
		QueryKey,
		[
			resolve: (message: OutputMessage) => void,
			reject: (error: unknown) => void
		]
	>();

	constructor(databasePath: string);
	constructor(config: ProcessorConfig);
	constructor(config: string | ProcessorConfig) {
		this.databasePath =
			typeof config === 'string' ? config : config.databasePath;

		if (
			typeof config !== 'string' &&
			config?.storage &&
			['local', 'session'].includes(config.storage)
		) {
			this.processor = new SQLocalProcessor();
			this.processor.onmessage = (message) => this.processMessageEvent(message);
		} else {
			this.processor = new Worker(new URL('./worker', import.meta.url), {
				type: 'module',
			});
			this.processor.addEventListener('message', this.processMessageEvent);
		}

		this.processor.postMessage({
			type: 'config',
			config: typeof config === 'string' ? { databasePath: config } : config,
		} satisfies ConfigMessage);
	}

	protected processMessageEvent = (
		event: OutputMessage | MessageEvent<OutputMessage>
	) => {
		const message = event instanceof MessageEvent ? event.data : event;
		const queries = this.queriesInProgress;

		switch (message.type) {
			case 'success':
			case 'data':
			case 'error':
				if (message.queryKey && queries.has(message.queryKey)) {
					const [resolve, reject] = queries.get(message.queryKey)!;
					if (message.type === 'error') {
						reject(message.error);
					} else {
						resolve(message);
					}
					queries.delete(message.queryKey);
				} else if (message.type === 'error') {
					throw message.error;
				}
				break;

			case 'callback':
				const userCallback = this.userCallbacks.get(message.name);

				if (userCallback) {
					userCallback(...(message.args ?? []));
				}
				break;
		}
	};

	protected createQuery = (
		message: OmitQueryKey<
			QueryMessage | TransactionMessage | DestroyMessage | FunctionMessage
		>
	) => {
		if (this.isDestroyed === true) {
			throw new Error(
				'This SQLocal client has been destroyed. You will need to initialize a new client in order to make further queries.'
			);
		}

		const queryKey = nanoid() satisfies QueryKey;

		this.processor.postMessage({
			...message,
			queryKey,
		} satisfies QueryMessage | TransactionMessage | DestroyMessage | FunctionMessage);

		return new Promise<OutputMessage>((resolve, reject) => {
			this.queriesInProgress.set(queryKey, [resolve, reject]);
		});
	};

	protected convertSqlTemplate = (
		queryTemplate: TemplateStringsArray,
		...params: any[]
	) => {
		return {
			sql: queryTemplate.join('?'),
			params,
		};
	};

	protected convertRowsToObjects = (rows: any[], columns: string[]) => {
		return rows.map((row) => {
			const rowObj = {} as Record<string, any>;
			columns.forEach((column, columnIndex) => {
				rowObj[column] = row[columnIndex];
			});
			return rowObj;
		});
	};

	protected exec = async (
		sql: string,
		params: any[],
		method: Sqlite3Method
	) => {
		const message = await this.createQuery({
			type: 'query',
			sql,
			params,
			method,
		});

		let data = {
			rows: [] as any[],
			columns: [] as string[],
		};

		if (message.type === 'data') {
			data.rows = message.rows;
			data.columns = message.columns;
		}

		return data;
	};

	sql = async <T extends Record<string, any>[]>(
		queryTemplate: TemplateStringsArray,
		...params: any[]
	) => {
		const statement = this.convertSqlTemplate(queryTemplate, ...params);
		const { rows, columns } = await this.exec(
			statement.sql,
			statement.params,
			'all'
		);
		return this.convertRowsToObjects(rows, columns) as T;
	};

	transaction = async (
		passStatements: (
			sql: SQLocal['convertSqlTemplate']
		) => ReturnType<SQLocal['convertSqlTemplate']>[]
	) => {
		const statements = passStatements(this.convertSqlTemplate);
		await this.createQuery({
			type: 'transaction',
			statements,
		});
	};

	createCallbackFunction = async (
		functionName: string,
		handler: CallbackUserFunction['handler']
	) => {
		await this.createQuery({
			type: 'function',
			functionName,
		});

		this.userCallbacks.set(functionName, handler);
	};

	getDatabaseFile = async () => {
		if (this.databasePath === undefined) {
			throw new Error(
				'This database is stored in localStorage or sessionStorage, so there is no file to retrieve.'
			);
		}

		const opfs = await navigator.storage.getDirectory();
		const fileHandle = await opfs.getFileHandle(this.databasePath);
		return await fileHandle.getFile();
	};

	overwriteDatabaseFile = async (databaseFile: FileSystemWriteChunkType) => {
		if (this.databasePath === undefined) {
			throw new Error(
				'This database is stored in localStorage or sessionStorage, so it cannot be overwritten with a file.'
			);
		}

		const opfs = await navigator.storage.getDirectory();
		const fileHandle = await opfs.getFileHandle(this.databasePath, {
			create: true,
		});
		const fileWritable = await fileHandle.createWritable();
		await fileWritable.truncate(0);
		await fileWritable.write(databaseFile);
		await fileWritable.close();
	};

	destroy = async () => {
		await this.createQuery({ type: 'destroy' });

		if (this.processor instanceof Worker) {
			this.processor.removeEventListener('message', this.processMessageEvent);
			this.processor.terminate();
		}

		this.queriesInProgress.clear();
		this.userCallbacks.clear();
		this.isDestroyed = true;
	};
}
