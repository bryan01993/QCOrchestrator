import pandas as pd
import gcsfs
import logging


def remove_text_file_header_footer(
    input_file: str,
    header_rows: int = 0,
    footer_rows: int = 0,
    delimiter: str = ";",
    file_format: str = "csv",
    encoding="latin1",
    chunk_size: int = 1000000,
    temp_file: str = None,
) -> None:
    """
    Elimina las filas de encabezado y/o pie de página especificadas de un fichero "csv", "txt", "dat" en
    Google Cloud Storage (GCS).

    Esta función lee el fichero de entrada en chunks, elimina las filas de encabezado y/o pie de página
    especificadas, y escribe el resultado en un fichero temporal en GCS. Luego, renombra el fichero temporal
    al fichero original.

    Parameters:
    input_file (str): Ruta completa del fichero de entrada en GCS, incluyendo el nombre del bucket y del fichero.
    header_rows (int, optional): Número de filas de encabezado a eliminar del fichero. Default is 0.
    footer_rows (int, optional): Número de filas de pie de página a eliminar del fichero. Default is 0.
    chunk_size (int, optional): Número de filas por chunk para leer el fichero. Default is 1000000.
    temp_file (str, optional): Ruta completa del fichero temporal en GCS. Si no se especifica, se genera
                                una ruta temporal basada en el fichero de entrada.
    file_format (str, optional): Formato del fichero. Por defecto es "CSV".
    delimiter (str, optional): Delimitador utilizado en el fichero CSV. Por defecto es ";".
    encoding (str, optional): Codificación del fichero. Por defecto es "latin1".

    Returns:
    None: La función no retorna ningún valor, pero levanta una excepción en caso de error.

    Raises:
    Exception: Se levanta una excepción en caso de un error durante el procesamiento.

    Example:
    remove_csv_header_footer(
        'gs://my_bucket/my_file.csv',
        header_rows=1,
        footer_rows=1,
        chunk_size=1000000,
        temp_file='gs://my_bucket/my_temp_file.csv'
    )
    """
    logging.info(
        f"Iniciando remove_header_footer para {input_file}, encoding={encoding}, delimiter={delimiter}, "
        f"header_rows={header_rows}. footer_rows={footer_rows}, chunk_size={chunk_size}"
    )
    logging.info(
        f"Se remplaza delimiter={delimiter} por salto de linea para garantizar 1 row -> 1 column"
    )
    read_delimiter = r"\r\n|\r|\n"

    header_rows = int(header_rows)
    footer_rows = int(footer_rows)
    chunk_size = int(chunk_size)
    fs = gcsfs.GCSFileSystem()

    logging.basicConfig(level=logging.INFO)
    if file_format not in ["csv", "txt", "dat"]:
        logging.info("El formato de fichero no es CSV. Tarea finalizada.")
        return

    if header_rows == 0 and footer_rows == 0:
        logging.info(
            "No se solicitó la eliminación de encabezado ni pie de página. Tarea finalizada."
        )
        return

    if temp_file is None:
        temp_file = f"{input_file}.tmp"

    if fs.exists(temp_file):
        logging.info(f"Eliminando fichero temporal existente: {temp_file}")
        try:
            fs.rm(temp_file)
        except Exception as e:
            logging.error(f"Error al eliminar el fichero temporal: {str(e)}")
            raise e

    try:
        total_chunks = -1
        reader = pd.read_csv(
            input_file,
            chunksize=chunk_size,
            header=None,
            dtype=str,
            delimiter=read_delimiter,
            encoding=encoding,
        )
        total_chunks = sum(1 for _ in reader)

        logging.info(
            f"total_chunks={total_chunks} for chunk_size={chunk_size} will be processed"
        )

        with fs.open(temp_file, "w") as f_out:
            reader = pd.read_csv(
                input_file,
                chunksize=chunk_size,
                header=None,
                dtype=str,
                delimiter=read_delimiter,
                encoding=encoding,
            )
            for chunk_number, chunk in enumerate(reader):
                if chunk_number == 0:
                    logging.warning(
                        f"Removing header rows:\n{chunk.iloc[:header_rows]}"
                    )
                    chunk = chunk.iloc[header_rows:]

                if footer_rows > 0 and chunk_number == total_chunks - 1:
                    logging.warning(
                        f"Removing footer rows:\n{chunk.iloc[-footer_rows:]}"
                    )
                    chunk = chunk.iloc[:-footer_rows]

                chunk.to_csv(f_out, mode="a", header=False, index=False, sep=delimiter)

        fs.rename(temp_file, input_file)
        logging.info("Proceso completado exitosamente.")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise e
