

def tiff2pdf(filename, new_filename):
    """
   :param filename: original tiff filename to be converted
   :param new_filename: converted filename to be saved with
    """

    try:
        file_extension = filename.split(".")[1].lower()

        if file_extension in ('tif','tiff'):
            logging.info("Converting... .... TiFF file to PDF")

            cmd = [
                "tiff2pdf", "{}".format(filename), "-o", "{}".format(new_filename), "-p", "A4"
            ]

            result = sp.run(cmd)
            # result = sp.Popen(cmd, shell=True)
            # result.communicate()

            if result.returncode == 0:
                logger.info(
                    "Converted {} to {} successfully".format(filename, new_filename))
            elif result.returncode == 2:
                logger.info(
                    "PDF could not be genrated from {}".format(filename))
    except Exception as error:
        logger.info(
            "Exception occured {} while converting tiff to pdf....Skipping tiff. Check if a valid tiff was downloaded"
            .format(error))

def tiff2pdf_gs(filename, new_filename):
    """
   :param filename: original tiff filename to be converted
   :param new_filename: converted filename to be saved with
    """
    try:
        file_extension = filename.split(".")[1].lower()

        if file_extension in ('tif','tiff'):
            logging.info("Converting... .... TiFF file to PDF")

            cmd = [ "gs", "-dBATCH", "-dQUIET", "-dNOPAUSE", "-sPAPERSIZE=a4", "-sDEVICE=tiffg4", "-sOutputFile={}".format(new_filename), "-f",  "{}".format(filename)
                ]
            #gs -dBATCH -dQUIET -dNOPAUSE -sDEVICE=tiffg4 -sOutputFile=1962.pdf -f original.tif
            result = sp.run(cmd)
            # result = sp.Popen(cmd, shell=True)
            # result.communicate()

            if result.returncode == 0:
                logger.info(
                    "Converted {} to {} successfully".format(filename, new_filename))
            elif result.returncode == 2:
                logger.info(
                    "PDF could not be generated from {}".format(filename))
    except Exception as error:
        logger.info(
            "Exception occurred {} while converting tiff to pdf....Skipping tiff. Check if a valid tiff was downloaded"
            .format(error))

def converting_image_pdf(filename,
                         new_filename=None,
                         start_page=None,
                         end_page=None):
    '''
    Converting jpg,jpeg,bmp,png files to pdf as well as  tiff file to pdf
    :param filename:
    :return:
    '''
    try:
        file_extension = filename.split(".")[1].lower()
        if not new_filename:
            new_filename = filename.split('.')[0] + ".pdf"
        if file_extension in ('jpg', 'jpeg', 'bmp', 'png'):
            logger.info("converting image in to pdf ")
            im = Image.open(filename)
            if im.mode == "RGBA":
                im = im.convert("RGB")
            if not os.path.exists(new_filename):
                im.save(new_filename, "pdf", resolution=100.0)
        if file_extension in ('tif', 'tiff'):
            try:
                image = Image.open(filename)
                pages = []
                page_iterators = list(ImageSequence.Iterator(image))
                if not start_page or not end_page:
                    start_page = 1
                    end_page = len(page_iterators)
                page_iterators = []
                for index, page in enumerate(ImageSequence.Iterator(image)):#page_iterators:
                    if index == end_page:
                        break
                    pages.append(page.convert("L"))

                pages[start_page-1].save(new_filename, save_all=True, append_images=pages[start_page:end_page])
                pages = []
            except OSError as ose:
                logger.warning(ose)
                tiff2pdf(filename, new_filename)

        return new_filename
    except Exception as error:
        logger.error(error)

def converting_tiff_2_pdf(filename, new_filename=None, start_page=None, end_page=None):
   """
  :param filename: original tiff filename to be converted
  :param new_filename: converted filename to be saved with
  :param start_range: Split from page number
  :param end_range: Split to page number
  :return:
   """
   try:
       file_extension = filename.split(".")[1].lower()

       if file_extension in ('tif','tiff'):
           logging.info("Converting TiFF file to PDF")
           image = Image.open(filename)
           pages = []
           for page in ImageSequence.Iterator(image):
               pages.append(page.convert("L"))
           pages[start_page - 1].save(new_filename, save_all=True, append_images=pages[start_page:end_page])
           #logging.info("Sucessfully converted and split the file {} from page {} to {} " .format(filename, start_page, end_page))
           pages = []
           return new_filename
   except:
       logging.info('Something went wrong during conversion')

def split_pdf(filename, output_filename=None, start_range=None, end_range=None):
    """
    :param start_range: page number from where to start splitting
    :param end_range: page number from where to end splitting
    :return:
    """
    try:
        pdf_object = PdfFileReader(open(filename, "rb"))
        if pdf_object.isEncrypted:
            pdf_object.decrypt('')
        #print("Total No of Pages " , reader.getNumPages() )
        #print(" Volume of the File " , size(os.path.getsize(file_path)))

        output = PdfFileWriter()
        for x in range(int(start_range) - 1, int(end_range)):
            output.addPage(pdf_object.getPage(x))
        if not output_filename:
            output_file = open("outfile" + str(end_range), "wb")
        else:
            output_file = open(output_filename, "wb")
        output.write(output_file)
        output_file.close()
        return output_filename
    except IndexError:
        logger.error(
            "Please enter a valid start and end range for the PDF, it has only `{}` pages!!!"
            .format(pdf_object.getNumPages()))
    except Exception as error:
        logger.error(error)


def file_extension_check_sniffing(filename, extension='.csv', separator=','):
    try:
        filename_part = os.path.splitext(os.path.basename(filename))
        if filename_part[1] != extension:
            error_msg = "Not a valid extension for file: {}".format(filename)
            return False

#with open(filename, 'r') as fp:
#    data = fp.read()
        data, index_removed = clean_data(
            filename,
            data_to_clean='data needs to be cleaned',
            junk_line_remover=2)

        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(str(data))
        separated_by = dialect.delimiter
        if separated_by == separator:
            return True
        else:
            error_msg = "Not a valid delimiter for file: {}".format(filename)
            return False
    except Exception as error:
        logger.error(error)

def check_file_is_empty(filename):
    try:
        if os.stat(filename).st_size == 0:
            return True
        else:
            return False
    except Exception as error:
        logger.error(error)

def clean_data(filename, data_to_clean=None, junk_line_remover=None, restrict_columns=None):
    """
        Clean data by removing some specified data and removing null data
        :param filename: Local filename
        :param data_to_clean: Data to be removed | Default None
        :return: data
    """
    extra_columns = False
    data, index_removed = read_csvfile_csvreader(
        filename,
        file_mode='rb',
        headerless=False,
        stop_line=data_to_clean,
        junk_line_remover=junk_line_remover)
    if not data:
        return []
    if data_to_clean:
        while True:
            try:
                data_clean_index = data.index(data_to_clean)
                data.remove([data_to_clean])
                index_removed.append(data_clean_index)

            except ValueError:
                break

    cleaned_up_data = []
    if data:
        for index, item in enumerate(data):
            try:
                #@TODO: To be uncommented
                #null_cleared_data = list(filter(None, item))
                null_cleared_data = data
                if null_cleared_data:
                    try:
                        if restrict_columns:
                            extra_columns = True
                            item = item[:restrict_columns]
                        if len(item) > 2 and not item.count('') >= 4:
                            cleaned_up_data.append(item)

                    except Exception as error:
                        logger.warning(error)
                        continue

            except Exception as error:
                logger.warning(error)
                continue
        index_removed = [index + 1 for index in index_removed]
        data = []
        null_cleared_data = []
        if not extra_columns:
            return cleaned_up_data, index_removed
        else:
            return cleaned_up_data, index_removed, extra_columns

def clean_data_rows(data, data_to_clean=None, junk_line_remover=None):
    if not data:
        return []
    if data_to_clean:
        while True:
            try:
                data.remove([data_to_clean])
            except ValueError:
                break
    cleaned_up_data = []
    if data:
        for item in data:
            try:
                null_cleared_data = list([_f for _f in item if _f])
                if null_cleared_data:
                    try:
                        if len(null_cleared_data) > 2:
                            cleaned_up_data.append(item)
                    except Exception as error:
                        logger.warning(error)
                        continue

            except Exception as error:
                logger.warning(error)
                continue
        return cleaned_up_data

def file_extension_check(filename, extension='.csv', separator=','):

    list_delimiters = []
    exp = r'^|,|\t|\n|\r|!|@'
    try:

        filename_part = os.path.splitext(os.path.basename(filename))
        if filename_part[1] != extension:
            error_msg = "Not a valid extension for file: {}".format(filename)
            return False

        with open(filename, errors='ignore') as delimiter_file:
            delimiter_lines = delimiter_file.readlines()
            for i in delimiter_lines:
                m = re.findall(exp, i)
                list_delimiters.append(m)
        count_delimiters = list(itertools.chain.from_iterable(list_delimiters))
        count_delimiters = Counter(count_delimiters).most_common(1)
        if count_delimiters:
            separated_by = count_delimiters[0][0]
        else:
            return False
        if separated_by == separator:
            return True
        else:
            error_msg = "ERROR: Not a valid delimiter for file: {}. Please check the file".format(
                filename)
            logger.critical(error_msg)
            return False
    except Exception as error:
        logger.error(error)



def len_csv_column(filename, delimiter=',', ignore_col=None, data_frame=pd.DataFrame()):
    """
    Find the column width of CSV files based on high number of column split
    by delimiiters
    :param filename:
    :param delimiter:
    :return: column_len
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        if data_frame.empty:
            with open(filename, 'r') as fp:
                reader = csv.reader(fp, delimiter=delimiter, quoting=csv.QUOTE_NONE)
                results = list(reader)
        else:
            results = data_frame.fillna('').values.tolist()
        column_len = Counter(
            [len(data) for data in results if data]).most_common(5)
        column_len = column_len[0][0]
        # If index has been provided to ignore, remove those empty spaces from the results index
        if ignore_col:
            [
                results[index].pop(0) for index, data in enumerate(results)
                if len(data) > column_len for ind, dat in enumerate(data)
                if ind == 0 and not dat
            ]
            column_len = Counter(
                [len(data) for data in results if data]).most_common(1)[0][0]
        logger.info("The column length for {} is {}".format(
            filename, column_len))

    except IOError as ie:
        logger.error("File not found: {}".format(filename))
    except Exception as error:
        column_len = 0
        results = []
        logger.error(error)
    return results, column_len

def write_csv_file(output_filename,
                   final_results,
                   write_mode='w',
                   delimiter=None,
                   quote_char=None,
                   quoting=None):
    """
    Writing content as a final output file as CSV
    :param output_filename:
    :return:
    """
    try:
        with open(output_filename, write_mode) as fp:
            logger.info(
                "Writing results to CSV file: {} ".format(output_filename))
            if quote_char:
                quoting = csv.QUOTE_ALL
                delimiter = ","
                quotechar = quote_char
                writer = csv.writer(
                    fp,
                    delimiter=delimiter,
                    quotechar=quote_char,
                    quoting=quoting)
            else:
                writer = csv.writer(fp)
            writer.writerows(final_results)
    except IOError as ie:
        logger.error(ie)
    except Exception as error:
        logger.error(error)

def write_lists_csv(output_filename,
                    final_results,
                    write_mode='w',
                    delimiter=None,
                    quote_char=None,
                    quoting=None):

    # Write List of list to  CSV file
    try:
        with open(transaction_analysis_output, 'w') as fp:
            writer = csv.writer(fp)
        writer.writerows(final_res)
        logger.info("Writing results to CSV file: {} ".format(output_filename))
    except Exception as error:
        logger.error(error)


def download_file_local(url, image_file=None):
    """
    Download a particular file from Web and store locally
    :param url:
    :param location:
    :return:
    """
    if not image_file:
        image_file = "test.png"
    try:
        response = requests.get(url, allow_redirects=True)
        with open(image_file, 'w') as out_file:
            out_file.write(response.content)
            logger.info(
                "Successfully downloaded the image file: {} from url : "
                "{}".format(image_file, url))
            return image_file

        del response

    except Exception as error:
        logger.error(error)



def read_csvfile_csvreader(filename,
                           file_mode='rb',
                           headerless=False,
                           stop_line=None,
                           junk_line_remover=None):

    try:
        encoding_type = detect_encoding_file(filename)
        if encoding_type:
            with open(filename, encoding=encoding_type) as fp:
                reader = csv.reader(fp)
                data = list(reader)
        else:
            with open(filename, errors='ignore') as fp:
                reader = csv.reader(fp)
                data = list(reader)
        return data, []
    except Exception as error:
        #logger.error(error)
        with open(filename, errors='ignore') as fp:
            reader = csv.reader(fp)
            data = list(reader)
        return data, []

def read_csvfile_csvreader2(filename,
                            file_mode='rb',
                            headerless=False,
                            stop_line=None,
                            junk_line_remover=None):
    """
    Read a csv file using csv reader
    :param filename:
    :param headerless:
    :return:
    """
    try:
        data = []
        index_removed = []
        junk_lines_stopper = []
        with open(filename, file_mode) as fp:
            # reader = csv.reader(fp)
            reader = csv.reader(
                fp)  #csv.reader(codecs.iterdecode(fp, 'utf-8'))

            #if headerless:
            #    next(reader)
            for index, row in enumerate(reader):
                if not row or not any(row):
                    junk_lines_stopper.append(True)
                    index_removed.append(index)
                    #if len(junk_lines_stopper) >= junk_line_remover:
                    #    break
                if stop_line in row:
                    break
                data.append(row)
        return data, index_removed

    except Exception as error:
        logger.error(error)



def read_csvfile(filename,
                 extract_field_name=False,
                 field_index=None,
                 headerless=False):
    """
    Read CSV file and extract header fields if specified
    :param filename: Filename of CSV
    :param extract_field_name: True/False
    :param field_index: Returns index of the fieldname if specified
    :return: data/fields/filed_index_result
    """
    try:
        with open(filename, "r") as f:
            reader = csv.reader(f)
            if extract_field_name:
                fields = next(reader)
                if extract_field_name and field_index:
                    field_index_result = fields.index(extract_field_name)
                    return field_index_result
                return fields
            elif headerless:
                next(reader)
                data = [row for row in reader]
                return data
            else:
                data = [row for row in reader]
                return data
    except Exception as error:
        logger.error(error)




def detect_encoding_file(filename):
    """
    Detect File encoding type
    :param self:
    :param filename: Filename
    :return: Encoding Type(utf-8, ascii, iso-8859-1, latin1, cp1252)
    """
    try:
        with open(filename, 'rb') as f:
            result = chardet.detect(f.read())
        return result.get('encoding')
    except Exception as error:
        logger.error(error)



def write_list_dict_csv(data, filename):
    """
    Write list of dictionaries to CSV using Dictwriter
    :param data:
    :param filename
    :return:
    """
    try:
        keys = list(data[0].keys())
        with open(filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)
    except Exception as error:
        logger.error(error)

def write_text_file(data, filename, write_mode=None):
    """
    Write data to normal text file
    :param data: Data
    :param filename: Local filename
    :return: None
    """
    if not write_mode:
        write_mode = 'w'
    try:
        with open(filename, write_mode) as fp:
            fp.write(data)
    except Exception as error:
        logger.error(error)


def delete_files_with_extension(dir_name, extension_type):
    """
    Delete files with a particular extension in a directory
    :param dir_name: Directory Name - Absolute path/ Relative path
    :param extension_type: Type of file extension - '.txt|.csv|.zip etc'
    :return: None
    """

    try:
        director_list = os.listdir(dir_name)
        for item in director_list:
            if item.endswith(extension_type):
                os.remove(os.path.join(dir_name, item))
                logger.info("Removing `{}` from `{}`".format(item, dir_name))
    except Exception as error:
        logger.error(error)



def use_local_file(download_flag, local_filename, folder_pathname):
    if download_flag == 1:
        shutil.copyfile('r1.csv', local_filename)
    if download_flag == 2:
        shutil.copyfile('7_2_all_debit.csv', local_filename)
    if download_flag == 3:
        shutil.copyfile('7_2_all_credit.csv', local_filename)
        shutil.copy('output_new_monthly.csv', folder_pathname)
        shutil.copy('output_transaction_analysis.csv', folder_pathname)



def decode_password(pdf_file, password):
    """
    This function will decode a password protected file.
    :param pdf_file: input file path
    :param password: password required for opening the password protected file.
    """
    try:
        try:
            logger.info("File: {} has been picked for password decryption.".
                        format(pdf_file))
            if not os.path.isfile(pdf_file) or os.path.splitext(
                    os.path.basename(pdf_file))[1] != '.pdf':

                logger.warning(
                    "The input file: {} either is not a pdf file or does not exist."
                    .format(os.path.basename(pdf_file)))
                return pdf_file, 'file_error'

        except utils.PdfReadError as err:
            logger.error(err)
            return pdf_file, err.__str__()

        except NotImplementedError as err:
            logger.error(err)
            return pdf_file, err.__str__()

        except Exception as error:
            logger.error(error)
            return pdf_file, 'file_error'

        dest_file = "{}_decoded.pdf".format(
            os.path.splitext(os.path.basename(pdf_file))[0])
        with open(pdf_file, 'r') as input_file:
            with open(dest_file, 'w') as output_file:
                reader = PdfFileReader(input_file)
                try:
                    reader.decrypt(password)
                except KeyError:
                    return pdf_file, "not_password_protected"
                writer = PdfFileWriter()

                for i in range(reader.getNumPages()):
                    writer.addPage(reader.getPage(i))
                    writer.write(output_file)
            logger.info("File has been successfully decoded to....{}".format(
                dest_file))
        pdf_file = 'decrypted_{}'.format(pdf_file)
        os.rename(dest_file, pdf_file)
        try:
            os.remove(dest_file)
        except:
            pass
        logger.info("The password has been removed for `{}`".format(
            os.path.basename(pdf_file)))
        return pdf_file, "password_decrypted"

    except utils.PdfReadError as err:
        if err.__str__() == 'File has not been decrypted':
            return pdf_file, 'wrong_password'
        elif err.__str__() == 'EOF marker not found':
            return pdf_file, 'file_error'
        else:
            return pdf_file, err.__str__()
    except Exception as error:
        try:
            new_pdf_file = "decrypted_{}".format(pdf_file)
            res = sp.run([
                "qpdf", "--password={}".format(password), "--decrypt",
                "{}".format(pdf_file), new_pdf_file
            ])
            if res.returncode == 0:
                return new_pdf_file, "password_decrypted"
            elif res.returncode == 2:
                return pdf_file, "wrong_password"
            else:
                return pdf_file, "file_error"

        except Exception as error:
            logger.error(" Failed, exception has occurred:  {}".format(error))
        return pdf_file, error.__str__()

def combine_csv_to_xlsx(file_list, output_filename):
    """
    This function will consolidate csv to xlsx file
    :param file_list: list of csv files to be consolidated to xlsx
    :param output_filename: Filename of the output file
    usage: file_list = [('Roc-check.csv', 'Roc-check'), ('gst.csv', 'GST-Check')] ##<[(csv_filename, sheet_name)]
           output_filename = 'Financials.xlsx'
    """
    try:
        wb = Workbook()
        #file_list = [('Roc-check.csv', 'Roc-check'), ('gst.csv', 'GST-Check')]
        for index, item in enumerate(file_list):
            wb_write = wb.create_sheet(file_list[index][1])
            df = pd.read_csv(
                file_list[index][0],
                error_bad_lines=False,
                encoding="ISO-8859-1",
                delim_whitespace=False,
                quoting=0,
                delimiter=",")
            df = df.fillna(' ')
            df_list = df.values.tolist()
            df_header_list = df.columns.values.tolist()
            for i in range(len(df_header_list)):
                if 'Unnamed:' in df_header_list[i]:
                    df_header_list[i] = ' '
            df_list.insert(0, df_header_list)
            for row, value in enumerate(df_list):
                wb_write.append(value)
            wb.save(output_filename)
        wb_del = openpyxl.load_workbook(output_filename)
        sheet_del = wb_del.get_sheet_by_name('Sheet')
        wb_del.remove_sheet(sheet_del)
        wb_del.save(output_filename)

    except Exception as error:
        logger.error(error)




def filesize_pagecount(file_path):
    """
    Function will return the file size
    param: path of the file
    return file size and page number
    """
    try:

        if os.path.isfile(file_path):
            file_info = os.stat(file_path)
            #file_size = convert_bytes(file_info.st_size)
            file_info = file_info.st_size
            if not file_info:
                file_size = ''
            try:
                pdfinfo_output = sp.check_output(['pdfinfo', file_path])
                pdfinfo_str = pdfinfo_output.decode('ascii', errors='ignore')
                page_count = re.search(r'^Pages\:(.+)$', pdfinfo_str, flags=re.M | re.U)
                no_of_pages = int(page_count.group(1))
                if not no_of_pages:
                    no_of_pages = ''
                file_details = {'file_size': file_info,'page_count': no_of_pages}
                return file_details
            except Exception:
                file_details = {'file_size': '','page_count': ''}
                return file_details
        else:
            return {'file not found'}
    except Exception as error:
        logger.error(error)

def convert_gif_to_pdf(filename):
    try:
        im = Image.open(filename)
    except IOError:
        logger.info("Cant load", filename)
        gc.collect()
        sys.exit(1)

    i = 0
    mypalette = im.getpalette()
    imagelist = []

    try:
        while 1:
            im.putpalette(mypalette)
            new_im = Image.new("RGBA", im.size)
            new_im.paste(im)
            new_im.save(str(i) + '.png')
            logger.info("Extracting image number {}".format(i))
            imagelist.append(str(i) + '.png')

            i += 1
            im.seek(im.tell() + 1)
    except EOFError:
        pass

    try:
        output_file = "converted_{}.pdf".format(os.path.splitext(os.path.basename(filename))[0])
        x, y, w, h = 0, 0, 200, 250
        pdf = FPDF(orientation='P', unit='mm', format='A4')
        for image in iter(imagelist):
            pdf.add_page()
            pdf.image(image, x, y, w, h)
            os.remove(image)
            logger.info("Adding image number {} to create pdf".format(image))
        pdf.output(output_file, "F")
        logger.info("Created PDF {} from {} successfully ".format(output_file, filename))

        return output_file
    except Exception as err:
        logger.error(err)
        gc.collect()
