#include <cstdlib>
#include <string>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

#include <nlohmann/json.hpp>

#include "check_license.hpp"

#ifdef EXONDB_LICENSE_SERVER_URL
#define EXONDB_LICENSE_SERVER_URL_VALUE EXONDB_LICENSE_SERVER_URL
#else
#define EXONDB_LICENSE_SERVER_URL_VALUE "https://api.wheretrue.com"
#endif

namespace wtt01
{
    void LicenseCheck::ValidateLicense()
    {
        auto license_status = GetLicenseStatus();
        if (license_status != LicenseCheck::LicenseStatus::ACTIVE)
        {
            throw std::runtime_error(LicenseCheck::GetLicenseStatusString(license_status));
        }
    }

    std::string LicenseCheck::GetLicenseStatusString(LicenseCheck::LicenseStatus license_status)
    {

        switch (license_status)
        {
        case LicenseCheck::LicenseStatus::ACTIVE:
            return "ACTIVE";
        case LicenseCheck::LicenseStatus::INACTIVE:
            return "INACTIVE";
        case LicenseCheck::LicenseStatus::INVALID_ENV_VAR_CONFIGURATION:
            return "INVALID_ENV_VAR_CONFIGURATION";
        case LicenseCheck::LicenseStatus::SERVER_CONNECTION_ERROR:
            return "SERVER_CONNECTION_ERROR";
        case LicenseCheck::LicenseStatus::UNEXPECTED_HTTP_STATUS_ERROR:
            return "UNEXPECTED_HTTP_STATUS_ERROR";
        case LicenseCheck::LicenseStatus::LICENSE_NOT_FOUND:
            return "LICENSE_NOT_FOUND";
        default:
            throw std::runtime_error("Failed to get license status string");
        }

        return "UNEXPECTED_ERROR";
    };

    LicenseCheck::LicenseStatus LicenseCheck::GetLicenseStatus()
    {
        auto url = std::string(EXONDB_LICENSE_SERVER_URL_VALUE);

        auto license_id = std::getenv("EXONDB_LICENSE");
        if (license_id == nullptr)
        {
            std::cerr << "EXONDB_LICENSE is not set" << std::endl;
            return LicenseCheck::LicenseStatus::INVALID_ENV_VAR_CONFIGURATION;
        }

        // TODO: Add check the license_id is valid UUID via regex.
        nlohmann::json params = {{"license_id", license_id}};

        httplib::Client cli(url);
        auto res = cli.Post("/wtt/license/verify", params.dump(), "application/json");

        if (!res)
        {
            std::cerr << "Server connection error: " << res.error() << std::endl;
            return LicenseCheck::LicenseStatus::SERVER_CONNECTION_ERROR;
        }

        if (res->status == 404)
        {
            std::cerr << "License not found" << std::endl;
            return LicenseCheck::LicenseStatus::LICENSE_NOT_FOUND;
        }

        if (res->status != 200)
        {
            std::cerr << "Unexpected HTTP status: " << res->status << std::endl;
            return LicenseCheck::LicenseStatus::UNEXPECTED_HTTP_STATUS_ERROR;
        }

        // Parse the json response of the body into a json object.
        auto parsed_body = nlohmann::json::parse(res->body);

        auto status = parsed_body["status"];
        if (status != "active")
        {
            std::cerr << "Inactive license status: " << status << std::endl;
            return LicenseCheck::LicenseStatus::INACTIVE;
        }

        return LicenseCheck::LicenseStatus::ACTIVE;
    }
}
