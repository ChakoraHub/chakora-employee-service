import os

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv("TIMESHEET_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
EMPLOYEE_RESOURCES_URL = os.getenv(
    "EMPLOYEE_RESOURCES_URL",
    f"{BASE_URL}/employee-resources",
)
WAIT = 25
LOCAL_DEBUG_LOGIN = os.getenv("TIMESHEET_LOCAL_DEBUG_LOGIN", "false").lower() == "true"
EXPECT_MAINTENANCE_NOTICE = os.getenv("EXPECT_MAINTENANCE_NOTICE", "false").lower() == "true"


@pytest.fixture
def browser():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1000")

    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(2)
    yield driver
    driver.quit()


def wait_loaded(driver):
    WebDriverWait(driver, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def open_employee_resources(browser):
    if LOCAL_DEBUG_LOGIN:
        browser.get(f"{BASE_URL}/debug_login")
        wait_loaded(browser)

    browser.get(EMPLOYEE_RESOURCES_URL)
    wait_loaded(browser)


def open_timesheet_section(browser):
    open_employee_resources(browser)

    timesheet_nav = WebDriverWait(browser, WAIT).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "[data-section='timesheet']"))
    )
    timesheet_nav.click()

    WebDriverWait(browser, WAIT).until(
        lambda d: d.find_element(By.ID, "timesheet").value_of_css_property("display") != "none"
    )


def test_employee_resources_opens(browser):
    open_employee_resources(browser)

    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, ".content-section.active"))
    )

    assert "/employee-resources" in browser.current_url


def test_maintenance_notice_state(browser):
    open_employee_resources(browser)
    notice = browser.find_elements(By.ID, "employee-resources-maintenance-notice")

    if EXPECT_MAINTENANCE_NOTICE:
        assert notice, "Maintenance is expected to be ON, but the notice was not found."
        assert notice[0].is_displayed()
        assert "Scheduled maintenance notice" in notice[0].text
    else:
        assert not notice, "Maintenance is expected to be OFF, but the notice is present."


def test_timesheet_section_opens(browser):
    open_timesheet_section(browser)

    timesheet_section = browser.find_element(By.ID, "timesheet")
    assert timesheet_section.is_displayed()
    assert "Timesheet" in timesheet_section.text
    assert "Log your daily work hours" in timesheet_section.text


def test_timesheet_weekly_attendance_table(browser):
    open_timesheet_section(browser)

    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.ID, "tsTimeTable"))
    )

    assert browser.find_element(By.ID, "tsTableBody").is_displayed()
    assert browser.find_element(By.ID, "tsDayHeaderRow").is_displayed()
    assert browser.find_element(By.ID, "tsSubHeaderRow").is_displayed()


def test_timesheet_summary_and_footer(browser):
    open_timesheet_section(browser)

    for element_id in [
        "tsTotalHours",
        "tsFillStatus",
        "tsFooterLogin",
        "tsFooterLogout",
        "tsFooterDuration",
    ]:
        assert browser.find_element(By.ID, element_id).is_displayed()


def test_timesheet_action_buttons(browser):
    open_timesheet_section(browser)

    export_button = WebDriverWait(browser, WAIT).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Export CSV')]"))
    )
    timesheet_section = browser.find_element(By.ID, "timesheet")

    clear_button = timesheet_section.find_element(
        By.XPATH, ".//button[contains(., 'Clear today')]"
    )
    save_button = timesheet_section.find_element(
        By.XPATH, ".//button[contains(., 'Save')]"
    )

    assert export_button.is_displayed()
    assert clear_button.is_displayed()
    assert save_button.is_displayed()


def test_timesheet_input_controls(browser):
    open_timesheet_section(browser)

    WebDriverWait(browser, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#tsTableBody input"))
    )

    login_inputs = browser.find_elements(By.CSS_SELECTOR, "#tsTableBody input[id^='ts_login_']")
    logout_inputs = browser.find_elements(By.CSS_SELECTOR, "#tsTableBody input[id^='ts_logout_']")
    note_inputs = browser.find_elements(By.CSS_SELECTOR, "#tsTableBody input[id^='ts_note_']")

    assert len(login_inputs) == 7
    assert len(logout_inputs) == 7
    assert len(note_inputs) == 7
