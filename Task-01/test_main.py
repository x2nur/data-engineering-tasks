from io import BytesIO
from pathlib import Path
from zipfile import ZipFile
from unittest.mock import Mock, patch

import main


# use pytest tmp_path fixture
def test_main(tmp_path):
    uris = [
        "https://example.com/Test_ZipArchive_2023_Q4.zip"
    ]

    main.download_uris = uris
    main.TARGET_DIR = tmp_path

    filename = Path(uris[0].split(sep='/')[-1]).stem

    mem_file = BytesIO()
    csv_content = 'Stub text for a csv file'

    zip = ZipFile(mem_file, mode='w') 
    zip.writestr(f'{filename}.csv', csv_content)
    zip.close()

    mem_file.seek(0)

    with patch('main.req') as req:
        response = Mock()
        response.status_code = 200

        def iter_content(chunk_size):
            yield mem_file.read(chunk_size)

        response.iter_content.side_effect = iter_content

        req.get.return_value = response
         
        main.main()

        assert not (tmp_path / f'{filename}.zip').exists(), "zip file wasn't deleted"
        csv_path = (tmp_path / f'{filename}.csv')
        assert csv_path.exists(), "csv file doesn't exist"
        assert csv_path.read_text() == csv_content, "csv file integrity problem"
