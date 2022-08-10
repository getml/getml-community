package main

import (
	"fmt"
	"log"
	"monitor/authentication"
	"monitor/config"
	"monitor/data"
	"monitor/data/logview"
	"monitor/tcp"
	"monitor/track"
	"monitor/views"
	"net/http"
	"os"
	"strconv"
)

func main() {

	// -------------------------------------------------------------------------

	const versionNumber = "1.3.0"

	const version = "getml-" + versionNumber

	// -------------------------------------------------------------------------

	environment := config.Environment{}

	environment.GetFromFile("../environment.json")

	// -------------------------------------------------------------------------

	conf, appPID, err := makeConfig()

	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// -------------------------------------------------------------------------

	updateData := data.NewUpdateData(environment)

	if updateData != nil && updateData.HasExpired(versionNumber) {
		log.Println(version + " has expired. Please download the newest " +
			"version at www.getml.com.")
		return
	}

	// -------------------------------------------------------------------------

	projects := data.NewProjects(
		conf, &environment, updateData, versionNumber)

	// -------------------------------------------------------------------------
	// For monitoring CPU and memory consumption of the engine.

	cpuMonitor := logview.NewCPUMonitor()

	go cpuMonitor.MonitorCPUUsage()

	memoryMonitor := logview.NewMemoryMonitor()

	go memoryMonitor.MonitorMemoryUsage()

	// -------------------------------------------------------------------------
	// Used for authenticating users

	authenticator := authentication.NewAuthenticator(
		*conf, environment, projects.LicenseSeedDynamic)

	// -------------------------------------------------------------------------
	// Used for communication with the license server

	licenseChecker := views.NewLicenseChecker(
		authenticator, projects, environment.Monitor)

	// -------------------------------------------------------------------------
	// Initialize handlers

	// TODO: We will need this when
	// we have proper user management on firebase.
	// configurationsManager := views.NewConfigurationsManager(authenticator, projects)

	authenticationChecker := views.NewAuthenticationChecker(authenticator)

	cachedFileServer := views.NewCachedFileServer(projects)

	columnViewer := views.NewColumnViewer(projects)

	cookieSetter := views.NewCookieSetter(authenticator, projects)

	csrfErrorHandler := views.NewCSRFErrorHandler(projects)

	dataFrameLister := views.NewDataFrameLister(projects)

	dataBaseTableLister := views.NewDataBaseTableLister(projects)

	dataBaseTableViewer := views.NewDataBaseTableViewer(projects)

	dataFrameViewer := views.NewDataFrameViewer(projects)

	featureViewer := views.NewFeatureViewer(projects)

	httpEndpoint := views.NewHTTPEndpoint(projects)

	hyperOptRunListener := views.NewHyperOptRunListener(projects)

	hyperoptLister := views.NewHyperoptLister(projects)

	logger := views.NewLogger(200, projects)

	mainHandler := views.NewMainHandler(version, os.Args[1:], authenticator, projects)

	observer := views.NewObserver(cpuMonitor, memoryMonitor, projects)

	pipelineLister := views.NewPipelineLister(projects)

	pipelineViewer := views.NewPipelineViewer(projects)

	projectManager := views.NewProjectManager(projects)

	// signInPage := views.NewSignInPage(authenticator, projects)

	terminalHandler := views.NewTerminalHandler(cpuMonitor, memoryMonitor, projects)

	tester := views.NewTester()

	userManager := views.NewUserManager(authenticator, projects)

	// ---------------------------------------------------------------
	// This shuts down the monitor, even when the calling App or CLI
	// is killed or force quitted.

	if appPID != -1 {
		go mainHandler.ShutdownWhenAppDies(int32(appPID))
	}

	// -------------------------------------------------------------------------
	// The file server. All of these files are publicly available anyway, so no authentication is needed.

	http.HandleFunc("/", cachedFileServer.ServeFile)

	// -------------------------------------------------------------------------
	// These views do not have to be authenticated for obvious reasons.

	http.HandleFunc("/csrferror/", csrfErrorHandler.CSRFError)

	http.HandleFunc("/forbiddenremoteaccess/", views.ForbiddenRemoteAccess)

	http.HandleFunc("/isalive/", views.IsAlive)

	http.HandleFunc("/isauthenticated/", authenticationChecker.IsAuthenticated)

	http.HandleFunc("/verifytoken/", cookieSetter.VerifyToken)

	// -------------------------------------------------------------------------
	// GET requests for data

	http.HandleFunc("/calccategoricalcolumnfrequencies/", authenticator.Wrap(columnViewer.CalcCategoricalColumnFrequencies))

	http.HandleFunc("/calccategoricalcorrelation/", authenticator.Wrap(columnViewer.CalcCategoricalCorrelation))

	http.HandleFunc("/calccolumncrosscorrelation/", authenticator.Wrap(columnViewer.CalcColumnCrossCorrelation))

	http.HandleFunc("/calccolumndensities/", authenticator.Wrap(columnViewer.CalcColumnDensities))

	http.HandleFunc("/calccolumnsummary/", authenticator.Wrap(columnViewer.CalcColumnSummary))

	http.HandleFunc("/cleanuphyperopt/", authenticator.Wrap(hyperoptLister.CleanUpHyperopt))

	http.HandleFunc("/getaccuracycurve/", authenticator.Wrap(pipelineViewer.GetAccuracyCurve))

	http.HandleFunc("/getaccuracy/", authenticator.Wrap(pipelineLister.GetAccuracy))

	http.HandleFunc("/getauc/", authenticator.Wrap(pipelineLister.GetAUC))

	http.HandleFunc("/getaveragetargets/", authenticator.Wrap(featureViewer.GetAverageTargets))

	http.HandleFunc("/getcorrelations/", authenticator.Wrap(pipelineViewer.GetCorrelations))

	http.HandleFunc("/getcpusparkline/", authenticator.Wrap(observer.GetCPUSparkline))

	http.HandleFunc("/getcpuusage/", authenticator.Wrap(terminalHandler.GetCPUUsage))

	http.HandleFunc("/getcolumnimportances/", authenticator.Wrap(pipelineViewer.GetColumnImportances))

	http.HandleFunc("/getcolumndata/", authenticator.Wrap(columnViewer.GetColumnData))

	http.HandleFunc("/getcolumnsidebardata/", authenticator.Wrap(columnViewer.GetColumnSidebarData))

	http.HandleFunc("/getcrossentropy/", authenticator.Wrap(pipelineLister.GetCrossEntropy))

	http.HandleFunc("/getdatabasetablecontent/", authenticator.Wrap(dataBaseTableViewer.GetDataBaseTableContent))

	http.HandleFunc("/getdatabasetabledata/", authenticator.Wrap(dataBaseTableViewer.GetDatabaseTableData))

	http.HandleFunc("/getdatabasetablesidebardata/", authenticator.Wrap(dataBaseTableViewer.GetDataBaseTableSidebarData))

	http.HandleFunc("/getdataframecontent/", authenticator.Wrap(dataFrameViewer.GetDataFrameContent))

	http.HandleFunc("/getdataframedata/", authenticator.Wrap(dataFrameViewer.GetDataFrameData))

	http.HandleFunc("/getdataframesidebardata/", authenticator.Wrap(dataFrameViewer.GetDataFrameSidebarData))

	http.HandleFunc("/getdataframesdata/", authenticator.Wrap(dataFrameLister.GetDataFramesData))

	http.HandleFunc("/getdatamodel/", authenticator.Wrap(pipelineViewer.GetDataModel))

	http.HandleFunc("/getdbtableslister/", authenticator.Wrap(dataBaseTableLister.GetDBTablesLister))

	// TODO
	// http.HandleFunc("/getdefaultconfig/", authenticator.Wrap(configurationsManager.GetDefaultConfig))

	http.HandleFunc("/getdeployedpipelinesdata/", authenticator.Wrap(pipelineLister.GetDeployedPipelinesData))

	// TODO
	//http.HandleFunc("/geteditconfigurationsdata/", authenticator.Wrap(configurationsManager.GetEditConfigurationsData))

	http.HandleFunc("/getfeaturedata/", authenticator.Wrap(featureViewer.GetFeatureData))

	http.HandleFunc("/getfeaturedensities/", authenticator.Wrap(featureViewer.GetFeatureDensities))

	http.HandleFunc("/getfeaturelearnerhyperparameters/", authenticator.Wrap(pipelineViewer.GetFeatureLearnerHyperparameters))

	http.HandleFunc("/getfeatureimportances/", authenticator.Wrap(pipelineViewer.GetFeatureImportances))

	http.HandleFunc("/getfeaturetable/", authenticator.Wrap(pipelineViewer.GetFeatureTable))

	http.HandleFunc("/getfeatureviewsidebardata/", authenticator.Wrap(featureViewer.GetFeatureViewSidebarData))

	http.HandleFunc("/getfittedpipelinesdata/", authenticator.Wrap(pipelineLister.GetFittedPipelinesData))

	http.HandleFunc("/gethyperopttable/", authenticator.Wrap(hyperoptLister.GetHyperoptTable))

	http.HandleFunc("/getliftcurve/", authenticator.Wrap(pipelineViewer.GetLiftCurve))

	http.HandleFunc("/getlistpipelinesdata/", authenticator.Wrap(pipelineLister.GetListPipelinesData))

	http.HandleFunc("/getlog/", authenticator.Wrap(logger.GetLog))

	http.HandleFunc("/getmanageusersdata/", authenticator.Wrap(userManager.GetManageUsersData))

	http.HandleFunc("/getmae/", authenticator.Wrap(pipelineLister.GetMAE))

	http.HandleFunc("/getmemorysparkline/", authenticator.Wrap(observer.GetMemorySparkline))

	http.HandleFunc("/getmemoryusage/", authenticator.Wrap(terminalHandler.GetMemoryUsage))

	http.HandleFunc("/getpipelinedata/", authenticator.Wrap(pipelineViewer.GetPipelineData))

	http.HandleFunc("/getpipelineviewsidebardata/", authenticator.Wrap(pipelineViewer.GetPipelineViewSidebarData))

	http.HandleFunc("/getmemsize/", authenticator.Wrap(dataFrameLister.GetMemSize))

	http.HandleFunc("/getnumrows/", authenticator.Wrap(dataFrameLister.GetNumRows))

	http.HandleFunc("/getobserverdata/", authenticator.Wrap(observer.GetObserverData))

	http.HandleFunc("/getprecisionrecallcurve/", authenticator.Wrap(pipelineViewer.GetPrecisionRecallCurve))

	http.HandleFunc("/getpredictorhyperparameters/", authenticator.Wrap(pipelineViewer.GetPredictorHyperparameters))

	http.HandleFunc("/getpreprocessorhyperparameters/", authenticator.Wrap(pipelineViewer.GetPreprocessorHyperparameters))

	http.HandleFunc("/getroccurve/", authenticator.Wrap(pipelineViewer.GetROCCurve))

	http.HandleFunc("/getrmse/", authenticator.Wrap(pipelineLister.GetRMSE))

	http.HandleFunc("/getprojectdataframes/", authenticator.Wrap(dataFrameLister.GetProjectDataFrames))

	http.HandleFunc("/getprojectproperties/", authenticator.Wrap(projectManager.GetProjectProperties))

	http.HandleFunc("/getrsquared/", authenticator.Wrap(pipelineLister.GetRSquared))

	http.HandleFunc("/getscorehistory/", authenticator.Wrap(pipelineViewer.GetScoreHistory))

	http.HandleFunc("/getsqlcode/", authenticator.Wrap(featureViewer.GetSQLCode))

	http.HandleFunc("/getupdatedata/", authenticator.Wrap(observer.GetUpdateData))

	http.HandleFunc("/getusers/", authenticator.Wrap(userManager.GetUsers))

	http.HandleFunc("/listprojectsdata/", authenticator.Wrap(projectManager.ListProjectsData))

	http.HandleFunc("/listrunningprojects/", authenticator.Wrap(projectManager.ListRunningProjects))

	// -------------------------------------------------------------------------
	// POST requests for actions needs authentication and CSRF protection.

	http.HandleFunc("/adduser/", authenticator.Wrap(userManager.AddUser))

	http.HandleFunc("/createnewproject/", authenticator.Wrap(projectManager.CreateNewProject))

	// TODO
	// http.HandleFunc("/createtlsencryption/", authenticator.Wrap(csrfProtector.Wrap(configurationsManager.CreateTLSEncryption)))

	http.HandleFunc("/deleteproject/", authenticator.Wrap(projectManager.DeleteProject))

	http.HandleFunc("/deletedataframe/", authenticator.Wrap(dataFrameLister.DeleteDataFrame))

	http.HandleFunc(
		"/deletedataframefrommemonly/",
		authenticator.Wrap(dataFrameLister.DeleteDataFrameFromMemOnly))

	http.HandleFunc("/deletehyperopt/", authenticator.Wrap(hyperoptLister.DeleteHyperopt))

	http.HandleFunc("/deletepipeline/", authenticator.Wrap(pipelineLister.DeletePipeline))

	// TODO
	// http.HandleFunc("/deletetlsencryption/", authenticator.Wrap(csrfProtector.Wrap(configurationsManager.DeleteTLSEncryption)))

	http.HandleFunc("/deleteuser/", authenticator.Wrap(userManager.DeleteUser))

	http.HandleFunc("/deploypipeline/", authenticator.Wrap(pipelineLister.DeployPipeline))

	// TODO
	//http.HandleFunc("/editengineconfigurations/", authenticator.Wrap(
	//		csrfProtector.Wrap(configurationsManager.EditEngineConfigurations)))

	// TODO
	//	http.HandleFunc("/editmonitorconfigurations/", authenticator.Wrap(
	//		csrfProtector.Wrap(configurationsManager.EditMonitorConfigurations)))

	http.HandleFunc("/loaddataframe/", authenticator.Wrap(dataFrameLister.LoadDataFrame))

	http.HandleFunc("/refreshdb/", authenticator.Wrap(dataBaseTableLister.RefreshDB))

	http.HandleFunc("/restartproject/", authenticator.Wrap(projectManager.RestartProject))

	http.HandleFunc("/savedataframe/", authenticator.Wrap(dataFrameLister.SaveDataFrame))

	http.HandleFunc("/shutdown/", authenticator.Wrap(mainHandler.Shutdown))

	http.HandleFunc("/setproject/", authenticator.Wrap(projectManager.SetProject))

	http.HandleFunc("/suspendproject/", authenticator.Wrap(projectManager.SuspendProject))

	http.HandleFunc("/tosql/", authenticator.Wrap(pipelineLister.ToSQL))

	http.HandleFunc("/undeploypipeline/", authenticator.Wrap(pipelineLister.UndeployPipeline))

	// -------------------------------------------------------------------------
	// POST request for prediction

	http.HandleFunc("/predict/", httpEndpoint.Predict)

	http.HandleFunc("/transform/", httpEndpoint.Transform)

	// -------------------------------------------------------------------------
	// Trivial actions that do not need CSRF protection.

	http.HandleFunc("/runtests/", authenticator.Wrap(tester.RunTests))

	http.HandleFunc("/logout/", authenticator.Wrap(userManager.LogOut))

	// -------------------------------------------------------------------------

	go track.Event(
		authenticator.UserID(),
		"Launched monitor",
		environment.Monitor.LicenseSeedStatic,
		projects.LicenseSeedDynamic,
		environment.Monitor.EventURL,
	)

	// -------------------------------------------------------------------------

	// -------------------------------------------------------------------------

	printStartMessage(version)

	// -------------------------------------------------------------------------

	if conf.Monitor.LaunchBrowser {
		startBrowser(conf.Monitor)
	}

	// -------------------------------------------------------------------------

	// TODO
	// Launching the HTTPS proxy is commented out until
	// we have proper login.
	/*if !conf.Monitor.NoLogin {
	go proxy.LaunchHTTPSProxy(
		conf.Monitor.HTTPPort,
		conf.Monitor.HTTPSPort,
		conf.Monitor.AllowIPs)
	}*/

	// -------------------------------------------------------------------------

	tcpServer := tcp.NewServer(
		dataBaseTableViewer,
		dataFrameLister,
		hyperOptRunListener,
		hyperoptLister,
		licenseChecker,
		logger,
		mainHandler,
		pipelineLister,
		projectManager,
		projects,
		userManager)

	go tcpServer.Launch()

	// -------------------------------------------------------------------------

	host := func() string {
		if conf.Monitor.AllowRemoteIPs {
			return ":"
		}
		return "localhost:"
	}

	// -------------------------------------------------------------------------

	httpPort := conf.Monitor.HTTPPort

	if err := http.ListenAndServe(host()+strconv.Itoa(httpPort), nil); err != nil {
		log.Println(err.Error())
	}

	// -------------------------------------------------------------------------

}
