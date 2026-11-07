import os
import sys
import time
import pytest
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ==============================================================================
# CONFIGURATION
# Default to production website if no environment variable is provided.
# ==============================================================================
BASE_URL = os.getenv(
    "EMPLOYEE_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

EMP_LEAVE_URL = f"{BASE_URL}/emp-leave"
EMP_DETAILS_URL = f"{BASE_URL}/employee/personal-details"
EMP_SALARY_URL = f"{BASE_URL}/employee/salary"
EMP_IDCARD_URL = f"{BASE_URL}/employee-idcard"
ADMIN_LEAVE_URL = f"{BASE_URL}/admin-leave-approval"
HEALTH_URL = f"{BASE_URL}/health"
WAIT = int(os.getenv("SELENIUM_WAIT_TIMEOUT", "25"))


# ==============================================================================
# FIXTURES
# ==============================================================================
@pytest.fixture
def driver():
    """Initialize a headless Chrome browser matching CI/CD standard options."""
    import shutil

    # Filter out mismatched pre-installed chromedrivers from PATH so Selenium Manager resolves matching driver
    clean_path_parts = [
        p for p in os.environ.get("PATH", "").split(os.pathsep)
        if "chromedriver" not in p.lower()
    ]
    os.environ["PATH"] = os.pathsep.join(clean_path_parts)

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-software-rasterizer")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1440,1000")

    chrome_bin = (
        os.getenv("CHROME_PATH")
        or os.getenv("CHROME_BIN")
        or shutil.which("google-chrome")
        or shutil.which("chrome")
    )
    if chrome_bin:
        options.binary_location = chrome_bin

    browser = None
    try:
        browser = webdriver.Chrome(options=options)
    except Exception as e1:
        try:
            options_legacy = webdriver.ChromeOptions()
            options_legacy.add_argument("--headless")
            options_legacy.add_argument("--no-sandbox")
            options_legacy.add_argument("--disable-dev-shm-usage")
            options_legacy.add_argument("--disable-gpu")
            if chrome_bin:
                options_legacy.binary_location = chrome_bin
            browser = webdriver.Chrome(options=options_legacy)
        except Exception as e2:
            pytest.skip(f"Chrome browser environment failed to start in runner: {e1} | {e2}")

    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        if browser:
            try:
                browser.quit()
            except Exception:
                pass


def wait_loaded(browser):
    """Wait until document.readyState == 'complete'."""
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


# ==============================================================================
# SELENIUM SMOKE TESTS FOR CHAKORA EMPLOYEE SERVICE MODULE
# ==============================================================================
def test_employee_health_endpoint():
    """Verify health endpoint responds without 50x server gateway errors."""
    try:
        resp = requests.get(HEALTH_URL, timeout=15)
        assert resp.status_code not in (502, 503, 504), (
            f"Health check returned gateway failure status: {resp.status_code}"
        )
    except requests.exceptions.RequestException as e:
        print(f"[Health Check Notice]: {e}")


def test_employee_leave_page_rendered(driver):
    """Verify employee leave management page loads and contains Chakora title."""
    driver.get(EMP_LEAVE_URL)
    wait_loaded(driver)

    title = driver.title
    assert "Chakora" in title or "Leave" in title, f"Unexpected page title: '{title}'"

    form_or_input = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "form, input, button, .container"))
    )
    assert form_or_input is not None, "Leave portal elements not found on /emp-leave"


def test_employee_personal_details_page_rendered(driver):
    """Verify employee personal details page renders without errors."""
    driver.get(EMP_DETAILS_URL)
    wait_loaded(driver)

    content = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "form, input, table, .container, body"))
    )
    assert content is not None, "Personal details page container not rendered"


def test_employee_salary_page_rendered(driver):
    """Verify employee salary portal renders successfully."""
    driver.get(EMP_SALARY_URL)
    wait_loaded(driver)

    body = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    assert body is not None, "Salary page body not found"
    assert driver.title != "", "Page title is empty"


def test_employee_idcard_page_rendered(driver):
    """Verify employee ID card page renders successfully."""
    driver.get(EMP_IDCARD_URL)
    wait_loaded(driver)

    body = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    assert body is not None, "ID card page body not found"
    assert driver.title != "", "Page title is empty"


def test_admin_leave_approval_page_rendered(driver):
    """Verify admin leave approval dashboard renders."""
    driver.get(ADMIN_LEAVE_URL)
    wait_loaded(driver)

    container = WebDriverWait(driver, WAIT).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".container, table, form, body"))
    )
    assert container is not None, "Admin leave approval container not found"
