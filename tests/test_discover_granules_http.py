import json
import os
import pytest
import requests
import responses
from bs4 import BeautifulSoup

from task.discover_granules_http import DiscoverGranulesHTTP


THIS_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture(scope="function")
def discover_granules_http():
    def gen_dg_http(collection):
        with open(os.path.join(THIS_DIR, f'test_event_{collection}.json'), 'r', encoding='UTF-8') as test_event_file:
            ev = json.load(test_event_file)
        return DiscoverGranulesHTTP(ev, None)
    return gen_dg_http


def response_kwargs(provider, url):
    with open(os.path.join(THIS_DIR, f'test_page_{provider}.html'), 'r', encoding='UTF-8') as html_file:
        html_str = html_file.read()
    with open(os.path.join(THIS_DIR, f'head_responses_{provider}.json'), 'r', encoding='UTF-8') as header_file:
        header_json = json.load(header_file)['head_responses']

    response_list = []
    response_list.append({
        'method': responses.GET,
        'url': url,
        'body': html_str,
        'status': 200,
        'headers': header_json[0]
    })

    header_idx = 1
    html_content = BeautifulSoup(html_str, features='html.parser')
    for a_tag in html_content.find_all('a', href=True)[1:]:
        href = a_tag.get('href')
        if href not in url:
            subdir = a_tag.get('href').rstrip('/').rsplit('/', 1)[-1]
            child_url = f'{url.rstrip("/")}/{subdir}'
            response_list.append({
                'method': responses.HEAD,
                'url': child_url,
                'status': 200,
                'headers': header_json[header_idx]
            })
            header_idx += 1
    
    return response_list


def test_discover_granules(mocker, discover_granules_http):
    dg = discover_granules_http('remss')
    mock_discover = mocker.patch.object(dg, 'discover')
    mock_session = mocker.patch('requests.Session')
    dg.discover_granules()

    mock_session.assert_called()
    mock_discover.assert_called()


@responses.activate
def test_get_file_link_remss(discover_granules_http):
    dg = discover_granules_http('remss')
    response_list = response_kwargs('remss', dg.provider_url)
    for rsp in response_list:
        responses.add(**rsp)
    session = requests.Session()
    dg.discover(session)
    discover_count = len(dg.dbm.list_dict)
    
    assert discover_count == 3
